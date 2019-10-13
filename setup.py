from setuptools import setup

setup(
    name='EasyLambda',
    version='2.0.0',
    description='Easy Lambda Function Library',
    url='git@github.com:loyaltycorp/easy_lambda',
    author='Damian Sloane',
    author_email='damian.sloane@loyaltycorp.com.au',
    license='proprietary',
    packages=[
        'EasyCloudFormation',
        'EasyCloudWatch',
        'EasyFilesystem',
        'EasyLambda',
        'EasyLocalDisk',
        'EasyLog',
        'EasyPipeline',
        'EasyS3',
        'EasySecretsManager',
        'EasySftp',
        'EasyValidator'
    ],
    zip_safe=False,
    install_requires=[
        'boto',
        'boto3',
        'botocore',
        'pysftp',
        'paramiko'
    ]
)
