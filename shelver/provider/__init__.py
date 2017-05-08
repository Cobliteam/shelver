from .base import Provider
from .amazon import AmazonProvider
from .test import TestProvider

Provider.register(AmazonProvider)
Provider.register(TestProvider)
