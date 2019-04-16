import os
import logging
import json
import tempfile
import gzip
from collections import Mapping
from functools import partial, lru_cache

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from boto3.session import Session
from botocore.client import ClientError
from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.build import Builder
from shelver.errors import ConfigurationError
from shelver.util import wrap_as_coll, is_collection
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


class AmazonBuilder(Builder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._instance_profile = None

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

    def _create_instance_profile(self):
        prof_name = 'PackerBuild'
        role_name = 'PackerBuild'
        policy_name = 'PackerBuild'

        iam = self.registry.provider.aws('iam')
        try:
            profile = iam.get_instance_profile(InstanceProfileName=prof_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                profile = None
            else:
                raise

        if not profile:
            profile = iam.create_instance_profile(
                InstanceProfileName=prof_name)

            iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps({
                  "Version": "2012-10-17",
                  "Statement": {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                  }
                }))
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps({
                   "Version": "2012-10-17",
                   "Statement": [{
                      "Effect": "Allow",
                      "Action": [
                         "ec2:DescribeInstances",
                         "ec2:DescribeImages",
                         "ec2:DescribeTags",
                         "ec2:DescribeSnapshots"
                      ],
                      "Resource": "*"
                   }]
                }))
            iam.add_role_to_instance_profile(
                InstanceProfileName=prof_name,
                RoleName=role_name)

        return profile['InstanceProfile']['InstanceProfileName']

    async def get_instance_profile(self):
        if not self._instance_profile:
            prof = await self.delay(self._create_instance_profile)
            self._instance_profile = prof

        return self._instance_profile

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
            return ''

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

    async def get_template_context(self, image, version, archive, **kwargs):
        context = await super().get_template_context(
            image, version, archive, **kwargs)

        data_file = await self.get_user_data_file(image)
        prof = await self.get_instance_profile()

        context.update({
            'aws_user_data_file': data_file,
            'aws_instance_profile': prof
        })
        return context


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
