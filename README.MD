# Easy Lambda Function

This is a Python package designed to be used in Serverless Lambda functions. It's primary
purpose is to facilitate simple logging and access to AWS facilities.

## Installation

This package can be installed from the command line using PIP as follows:

`pip install git+ssh://git@github.com/loyaltycorp/easy_lambda.git`

## Basic Usage

Derive a new application specific class (e.g. MyApplication) that extends the EasyLambda
class.

```python
from EasyIterator.EasyIterator import EasyIterator

class MyApplication(EasyLambda):
    """
    Your application class
    """
```

Inside this class, override the run() method of EasyLambda

```python
from EasyIterator.EasyIterator import EasyIterator

class MyApplication(EasyLambda):
    """
    Your application class
    """
    def run(self):
        """
        Insert your application logic here
        
        :return: None 
        """
        pass
```