from setuptools import setup

setup(
    name='EasyLambda',
    version='1.0.0',
    description='Easy Lambda Function Library',
    url='git@github.com:loyaltycorp/easy_lambda',
    author='Damian Sloane',
    author_email='damian.sloane@loyaltycorp.com.au',
    license='proprietary',
    packages=['EasyLambda'],
    zip_safe=False,
    install_requires=[
        'boto',
        'boto3',
        'botocore',
        'EasyBoto3 @ git+ssh://git@github.com/loyaltycorp/easy_boto3.git@v2.0'
    ]
)
