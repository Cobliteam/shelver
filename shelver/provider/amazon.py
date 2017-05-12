import os
import logging
import json
import tempfile
import gzip
import asyncio
from collections import Mapping
from functools import partial, lru_cache

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
    def __init__(self, provider, ami, image=None):
        if image:
            name = None
            version = _get_tag_by_key(ami.tags, AMI_VERSION_TAG)
            environment = _get_tag_by_key(ami.tags, AMI_ENVIRONMENT_TAG)
        else:
            name = ami.name
            version = None
            environment = None

        super().__init__(provider, name=name, image=image, version=version,
                         environment=environment)

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

        return self.get_image(name_tag)

    def _register_ami(self, ami, image=None):
        artifact = AmazonArtifact(self.provider, ami, image=image)
        self.register_artifact(artifact)
        if image:
            self.associate_artifact(artifact, image=image)

        return artifact

    @asyncio.coroutine
    def load_artifact_by_id(self, id, region=None, image=None):
        ec2 = self.provider.aws_res('ec2')

        if region and region != self.provider.region:
            logger.warn(
                'Not loading AMI with ID %s, as it is not in region %s',
                id, region)
            return

        ami = ec2.Image(id)
        yield from self.delay(ami.load)
        return self._register_ami(ami, image)

    @asyncio.coroutine
    def load_existing_artifacts(self, region=None):
        ec2 = self.provider.aws_res('ec2')

        def load_images():
            images = ec2.images.filter(Owners=['self'],
                                       Filters=self.ami_filters)
            return list(images)

        images = yield from self.delay(load_images)
        for ami in images:
            image = self._get_image_for_ami(ami)
            self._register_ami(ami, image)

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

    @asyncio.coroutine
    def get_instance_profile(self):
        if not self._instance_profile:
            prof = yield from self.delay(self._create_instance_profile)
            self._instance_profile = prof

        return self._instance_profile

    @asyncio.coroutine
    def get_user_data_file(self, image):
        if not image.metadata:
            return ''

        tmp = yield from self.get_build_tmp_dir()
        fd, path = yield from self.delay(
            partial(tempfile.mkstemp, suffix='.gz', dir=tmp))

        data = '\n'.join(image.metadata).encode('utf-8')
        with os.fdopen(fd, 'wb') as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gzf:
                yield from self.delay(gzf.write, data)

        return path

    @asyncio.coroutine
    def get_template_context(self, image, version, archive, **kwargs):
        context = yield from super().get_template_context(
            image, version, archive, **kwargs)

        data_file = yield from self.get_user_data_file(image)
        prof = yield from self.get_instance_profile()

        context.update({
            'aws_user_data_file': data_file,
            'aws_instance_profile': prof
        })
        return context


class AmazonProvider(Provider):
    NAMES = ('amazon', 'aws')

    Registry = AmazonRegistry
    Builder = AmazonBuilder
    Artifact = AmazonArtifact

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
