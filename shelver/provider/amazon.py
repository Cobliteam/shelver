from __future__ import absolute_import, unicode_literals

import os
import tempfile
import gzip
import logging
from collections import Mapping
try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

from boto3.session import Session
from shelver.provider import Provider
from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.builder import Builder
from shelver.util import wrap_as_coll, is_collection


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

        super(AmazonArtifact, self).__init__(
            provider, name=name, image=image, version=version,
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
            raise ValueError('AMI filters must be a list or dict')

    def __init__(self, provider, data, ami_filters=None):
        super(AmazonRegistry, self).__init__(provider, data)

        self.region = provider.region
        self.ami_filters = self.prepare_ami_filters(ami_filters)

    def _get_image_for_ami(self, ami):
        name_tag = _get_tag_by_key(ami.tags, AMI_NAME_TAG)
        if not name_tag:
            return None

        return self.get_image(name_tag)

    def _register_ami(self, ami, image=None):
        if not image:
            image = self._get_image_for_ami(ami)

        artifact = AmazonArtifact(self.provider, ami, image=image)
        if image:
            self.register_image_artifact(image, artifact.version, artifact)
        else:
            self.register_artifact(artifact)

    def load_artifact_by_id(self, id, region=None, image=None):
        ec2 = self.provider.aws_res('ec2')

        if region and region != self.provider.region:
            logger.warn('Not loading AMI with ID %s, as it is not in region %s',
                id, region)
            return

        ami = ec2.Image(id)
        ami.load()
        self._register_ami(ami, image)

    def load_existing_artifacts(self):
        ec2 = self.provider.aws_res('ec2')

        for ami in ec2.images.filter(Owners=['self'], Filters=self.ami_filters):
            self._register_ami(ami)

class AmazonBuilder(Builder):
    def __init__(self, *args, **kwargs):
        super(AmazonBuilder, self).__init__(*args, **kwargs)

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
            profile = iam.create_instance_profile(InstanceProfileName=prof_name)
            role = iam.create_role(
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

    def get_instance_profile(self):
        if not self._instance_profile:
            self._instance_profile = self._create_instance_profile()

        return self._instance_profile

    def get_user_data_file(self, image):
        if not image.metadata:
            return ''

        fd, user_data_file = tempfile.mkstemp(
            suffix='.gz', dir=self.get_build_tmp_dir())
        with os.fdopen(fd, 'wb') as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gzf:
                gzf.write('\n'.join(image.metadata))

        return user_data_file

    def template_context(self, image, version):
        context = super(AmazonBuilder, self).template_context(image, version)
        context.update({
            'user_data_file': self.get_user_data_file(image),
            'instance_profile': self.get_instance_profile()
        })
        return context

class AmazonProvider(Provider):
    NAMES = ('amazon', 'aws')

    Registry = AmazonRegistry
    Builder = AmazonBuilder
    Artifact = AmazonArtifact

    def __init__(self, config):
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


