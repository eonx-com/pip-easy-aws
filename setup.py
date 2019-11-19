from setuptools import setup

setup(
    name='EasyAws',
    version='1.0.0',
    description='Easy AWS Function Library',
    url='https://github.com/eonx-com/pip-easy-aws.git',
    author='Damian Sloane',
    author_email='damian.sloane@eonx.com.au',
    license='proprietary',
    packages=[
        'EasyCloudFormation',
        'EasyCloudWatch',
        'EasyFilesystem',
        'EasyFilesystem.S3',
        'EasyFilesystem.Sftp',
        'EasyLocalDisk',
        'EasyLog',
        'EasyOrganization',
        'EasyPipeline',
        'EasySecretsManager'
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
