from setuptools import setup
import flexi_config

setup(
    name='flexi-config',
    version=flexi_config.__version__,
    description='Flexible config objects utilizing AWS Secrets Manager',
    author="Ryan Beaman",
    author_email="ryan.b@leantaas.com",
    install_requires=[
        'requests>=2.3.0',
        'six>=1.10.0',
        'boto3>=1.9.231',
        'botocore>=1.12.231',
        'docutils>=0.15.2',
        'jmespath>=0.9.4',
        'python-dateutil>=2.8.0',
        'PyYAML>=5.1.2',
        's3transfer>=0.2.1',
        'six>=1.12.0',
        'urllib3>=1.25.3'
    ],
    packages=['flexi_config'])