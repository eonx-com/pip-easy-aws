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
from Iterator.EasyIterator import EasyIterator

class MyApplication(EasyLambda):
    """
    Your application class
    """
```

Inside this class, override the run() method of EasyLambda

```python
from Iterator.EasyIterator import EasyIterator

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

## Logging

##### Setting Log Level

To set the logging level call the `set_log_level()` function passing in an integer value:

* 0 (Standard Application Logging)
* 1 (Warning Logging)
* 2 (Verbose Logging)
* 3 (Trace Logging)

```python
self.set_log_level(3)
```

You can then use the `log_xxx()` functions in your code to log messages.

* 0 (log)
* 1 (log_warning, log)
* 2 (log_debug, log_warning, log)
* 3 (log_trace, log_debug, log_warning, log)
