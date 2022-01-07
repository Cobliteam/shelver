import datetime
import gzip
import json
import logging
import os
import re
import tempfile

from collections import Mapping
from functools import partial, lru_cache

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from boto3.session import Session
from botocore.exceptions import ClientError, WaiterError

from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.build import Builder
from shelver.errors import ConfigurationError
from shelver.util import JSONEncoder, wrap_as_coll, is_collection
from .base import Provider


AMI_NAME_TAG = 'ImageName'
AMI_VERSION_TAG = 'ImageVersion'
AMI_ENVIRONMENT_TAG = 'ImageEnvironment'

logger = logging.getLogger('shelver.provider.amazon')


def _get_tag_by_key(tags, key):
    if not tags:
        return None

    return next((tag['Value'] for tag in tags if tag['Key'] == key), None)


class AmazonArtifact(Artifact):
    def __init__(self, ami, image=None, **kwargs):
        if image:
            kwargs['version'] = _get_tag_by_key(ami.tags, AMI_VERSION_TAG)
            kwargs['environment'] = _get_tag_by_key(ami.tags,
                                                    AMI_ENVIRONMENT_TAG)
        else:
            kwargs['name'] = ami.name
            kwargs['version'] = None
            kwargs['environment'] = None

        super().__init__(image=image, **kwargs)

        self._ami = ami

    @property
    def id(self):
        return self.ami.image_id

    @property
    def ami(self):
        return self._ami


class AmazonRegistry(Registry):
    @staticmethod
    def prepare_ami_filters(filters):
        if filters is None:
            return tuple()
        elif isinstance(filters, Mapping):
            return tuple({'Name': k, 'Values': wrap_as_coll(v, tuple)}
                         for k, v in filters)
        elif is_collection(filters):
            return filters
        else:
            raise ConfigurationError('AMI filters must be a list or dict')

    def __init__(self, *args, ami_filters=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.region = self.provider.region
        self.ami_filters = self.prepare_ami_filters(ami_filters)

    def _get_image_for_ami(self, ami):
        name_tag = _get_tag_by_key(ami.tags, AMI_NAME_TAG)
        if not name_tag:
            return None

        image = self.get_image(name_tag, default=None)
        if not image:
            logger.warn('Ignoring artifact association for missing image `%s`',
                        name_tag)
            return None

        return image

    def _register_ami(self, ami, image=None):
        if not image:
            image = self._get_image_for_ami(ami)

        artifact = AmazonArtifact(ami, image=image, provider=self.provider)
        self.register_artifact(artifact)
        if image:
            self.associate_artifact(artifact, image=image)

        return artifact

    async def load_artifact_by_id(self, id, region=None, image=None):
        ec2 = self.provider.aws_res('ec2')

        if region and region != self.provider.region:
            logger.warn(
                'Not loading AMI with ID %s, as it is not in region %s',
                id, region)
            return

        ami = ec2.Image(id)
        await self.delay(ami.load)
        return self._register_ami(ami, image)

    async def load_existing_artifacts(self, region=None):
        logger.info('Loading existing AMIs from EC2')
        ec2 = self.provider.aws_res('ec2')

        def load_images():
            images = ec2.images.filter(Owners=['self'],
                                       Filters=self.ami_filters)
            return list(images)

        images = await self.delay(load_images)
        for ami in images:
            logger.debug('Registering AMI: %s', ami.id)
            self._register_ami(ami)

        return self


def change_set_response_up_to_date(response):
    status = response["Status"]

    if status == "FAILED":
        status_reason = response["StatusReason"]
        if ("didn't contain changes" in status_reason or
                "No updates are to be performed" in status_reason):
            return True

    return False


def deploy_cloudformation_stack(session, stack_name, template,
                                parameters=None, capabilities=None,
                                tags=None):
    cfn = session.client('cloudformation')

    if not isinstance(template, str):
        template = json.dumps(template, cls=JSONEncoder)

    cfn.validate_template(TemplateBody=template)

    tags = tags or {}
    stack_tags = [dict(Key=key, Value=value) for (key, value) in tags.items()]

    now = datetime.datetime.utcnow()
    date_str = now.strftime('%Y%m%d%H%M%S')
    change_set_name = '{}-{}'.format(stack_name, date_str)

    stack_capabilities = set(capabilities or [])
    stack_parameters = {}

    try:
        response = cfn.describe_stacks(StackName=stack_name)
        stack = response['Stacks'][0]

        stack_capabilities.update(stack['Capabilities'])

        for param in stack['Parameters']:
            key = param['ParameterKey']
            stack_parameters[key] = dict(ParameterKey=key,
                                         UsePreviousValue=True)

        execute_waiter = cfn.get_waiter('stack_update_complete')
        change_set_type = 'UPDATE'
    except ClientError as err:
        if err.response['Error']['Code'] == 'ValidationError' \
                and 'does not exist' in err.response['Error']['Message']:
            execute_waiter = cfn.get_waiter('stack_create_complete')
            change_set_type = 'CREATE'
        else:
            raise

    for key, override in parameters.items():
        stack_parameters[key] = dict(ParameterKey=key,
                                     ParameterValue=override)

    change_set = cfn.create_change_set(
        ChangeSetName=change_set_name,
        ChangeSetType=change_set_type,
        StackName=stack_name,
        TemplateBody=template,
        Parameters=list(stack_parameters.values()),
        Tags=stack_tags,
        Capabilities=list(stack_capabilities))

    up_to_date = False
    try:
        cfn.get_waiter('change_set_create_complete').wait(
            ChangeSetName=change_set['Id'],
            StackName=change_set['StackId'])
    except WaiterError as err:
        response = err.last_response
        if not change_set_response_up_to_date(response):
            raise

        up_to_date = True

    if not up_to_date:
        cfn.execute_change_set(
            ChangeSetName=change_set['Id'],
            StackName=change_set['StackId'])

        execute_waiter.wait(StackName=change_set['StackId'])

    response = cfn.describe_stacks(StackName=stack_name)
    stack = response['Stacks'][0]
    return stack


instance_profile_template = """
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  PolicyDocument:
    Type: String
Outputs:
  RoleName:
    Value: !Ref "PackerBuildRole"
  RoleArn:
    Value: !GetAtt ["PackerBuildRole", "Arn"]
  InstanceProfileName:
    Value: !Ref "PackerBuildInstanceProfile"
  InstanceProfileArn:
    Value: !GetAtt ["PackerBuildInstanceProfile", "Arn"]
Resources:
  PackerBuildRole:
    Type: "AWS::IAM::Role"
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: "Allow"
            Principal:
              Service:
                - "ec2.amazonaws.com"
            Action:
              - "sts:AssumeRole"
      Path: "/"
      Policies:
        - PolicyName: "PackerBuild"
          PolicyDocument: !Sub "${PolicyDocument}"
  PackerBuildInstanceProfile:
    Type: "AWS::IAM::InstanceProfile"
    Properties:
      Path: "/"
      Roles:
        - Ref: "PackerBuildRole"
"""


class AmazonBuilder(Builder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._instance_profile_arn = None

    def find_running_build(self, image, version):
        ec2 = self.registry.provider.aws('ec2')
        result = ec2.describe_instances(DryRun=False, Filters=[
            {'Name': 'tag:ImageName', 'Values': [image.name]},
            {'Name': 'tag:ImageVersion', 'Values': [version]},
            {'Name': 'instance-state-name', 'Values': ['running', 'stopping']}
        ])

        reservations = result['Reservations']
        if reservations:
            instances = reservations[0]['Instances']
            if instances:
                return instances[0]['InstanceId']

        return None

    def _create_instance_profile(self, name, policy_document):
        if isinstance(policy_document, (bytes, str)):
            pass
        elif isinstance(policy_document, Mapping):
            policy_document = json.dumps(
                policy_document, sort_keys=True, cls=JSONEncoder)
        else:
            raise ValueError(
                'Policy document must be a string or dict, not {}'.format(
                    type(policy_document)))

        session = self.registry.provider.session
        clean_name = re.sub(r'[^a-zA-Z0-9-]+', '-', name)

        stack = deploy_cloudformation_stack(
            session,
            stack_name='packer-{}-instance-profile'.format(clean_name),
            template=instance_profile_template,
            parameters=dict(PolicyDocument=policy_document),
            capabilities=["CAPABILITY_NAMED_IAM"])

        arn_output_name = 'InstanceProfileArn'
        for output in stack['Outputs']:
            if output['OutputKey'] == arn_output_name:
                arn = output['OutputValue']
                return arn

        raise RuntimeError(
            'Expected output {} not present in stack {}'.format(
                arn_output_name, stack['StackName']))

    async def get_instance_profile_arn(self, image):
        opts = image.provider_options.get('instance_profile', {})
        arn = opts.get('arn')
        if not arn:
            policy_doc = opts.get('policy_document')
            if policy_doc:
                arn = await self.delay(self._create_instance_profile,
                                       image.name, policy_doc)

        return arn

    def _guess_userdata_type(self, data):
        mime_types = {
            '#!':              'text/x-shellscript',
            '#cloud-config':   'text/cloud-config',
            '#upstart-job':    'text/upstart-job',
            '#cloud-boothook': 'text/cloud-boothook',
            '#part-handler':   'text/part-handler',
            '#include':        'text/x-include-url'
        }

        for prefix, mime_type in mime_types.items():
            if data.startswith(prefix):
                return mime_type

        return None

    def _encode_userdata_multipart(self, parts):
        message = MIMEMultipart()
        for part in parts:
            mime_type = self._guess_userdata_type(part)
            if not mime_type:
                raise ValueError('Failed to guess MIME type for userdata part')

            message_part = MIMEText(part, mime_type)
            message.attach(message_part)

        return bytes(message)

    async def get_user_data_file(self, image):
        if not image.metadata:
            return None

        tmp = await self.get_build_tmp_dir()
        fd, path = await self.delay(
            partial(tempfile.mkstemp, suffix='.gz', dir=tmp))

        if len(image.metadata) == 1:
            data = image.metadata[0].encode('utf-8')
        else:
            data = self._encode_userdata_multipart(image.metadata)

        with os.fdopen(fd, 'wb') as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gzf:
                await self.delay(gzf.write, data)

        return path

    async def _get_build_env(self):
        env = await super(AmazonBuilder, self)._get_build_env()
        session = self.registry.provider.session
        creds = session.get_credentials().get_frozen_credentials()
        env.update(AWS_ACCESS_KEY_ID=creds.access_key,
                   AWS_SECRET_ACCESS_KEY=creds.secret_key,
                   AWS_SESSION_TOKEN=creds.token)
        return env

    async def post_process_template(self, data, image):
        data = \
            await super(AmazonBuilder, self).post_process_template(data, image)

        user_data_file = await self.get_user_data_file(image)
        profile_arn = await self.get_instance_profile_arn(image)
        _, profile_name = profile_arn.rsplit('/', 1)
        overrides = dict(
            user_data_file=user_data_file,
            nao_fech_este_pr=profile_name
        )

        return self._apply_builder_overrides(data, overrides)


class AmazonProvider(Provider):
    name = 'amazon'
    aliases = ('aws',)

    registry_class = AmazonRegistry
    builder_class = AmazonBuilder
    artifact_class = AmazonArtifact

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)

        region = config.pop('region', None)
        if region:
            config = config.copy()
            config['region_name'] = region

        self.instance_profile = config.pop('instance_profile', {})

        self._session = Session(**config)
        self.region = self._session.region_name

        self.aws = lru_cache()(self._get_client)
        self.aws_res = lru_cache()(self._get_resource)

    @property
    def session(self):
        return self._session

    def _get_client(self, *args, **kwargs):
        return self.session.client(*args, **kwargs)

    def _get_resource(self, *args, **kwargs):
        return self.session.resource(*args, **kwargs)
