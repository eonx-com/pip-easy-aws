# Easy Lambda Function

This is a Python package designed to be used in Serverless Lambda functions to make life easier.

## Installation

This package can be installed from the command line using PIP as follows:

`pip install git+ssh://git@github.com/loyaltycorp/easy_lambda.git@v3.0`

## Basic Usage

Derive a new application specific class (e.g. MyApplication) that extends the EasyLambda
class.

```python
from EasyLambda.EasyLambda import EasyLambda

class MyApplication(EasyLambda):
    """
    Your application class
    """
```

Inside this class, override the run() method of EasyLambda

```python
from EasyLambda.EasyLambda import EasyLambda

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

To set the logging level call the `set_log_level()` function passing in one of the
EasyLog.LEVEL_XXXXX constants:

* EasyLog.LEVEL_INFO 
* EasyLog.LEVEL_WARNING
* EasyLog.LEVEL_DEBUG
* EasyLog.LEVEL_TRACE

```python
self.set_log_level(EasyLog.LEVEL_DEBUG)
```

If no level is defined it will automatically default to LEVEL_TRACE (the most verbose)

You can then use any of the `log_xxxxx()` functions in your code to log messages with the 
appropriate level.
