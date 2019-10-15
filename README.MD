# Easy Lambda

This is a collection of Python classes designed to be used in Serverless Lambda functions to make life easier.

## Installation

This package can be installed from the command line using PIP as follows:

`pip install git+ssh://git@github.com/loyaltycorp/easy_lambda.git@v3.0`

# EasyCloudFormation

Documentation to be done

# EasyCloudWatch

Documentation to be done

# EasyFilesystem

Documentation to be done


# EasyLambda

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

# EasyLocalDisk

Documentation to be done

# EasyLog

##### Setting Log Level

To set the logging level call the `Log.set_level()` function passing in one of the
Log.LEVEL_XXXXX constants:

* Log.LEVEL_EXCEPTION 
* Log.LEVEL_TEST 
* Log.LEVEL_ERROR
* Log.LEVEL_INFO 
* Log.LEVEL_WARNING
* Log.LEVEL_DEBUG
* Log.LEVEL_TRACE

```python
Log.set_level(Log.LEVEL_DEBUG)
```

If no level is defined it will automatically default to LEVEL_TRACE (the most verbose)

You can then use any of the `Log.xxxxx()` functions, for example `Log.debug('message')`

# EasyPipeline

Documentation to be done

# EasySecretsManager

Documentation to be done

# EasyValidator

