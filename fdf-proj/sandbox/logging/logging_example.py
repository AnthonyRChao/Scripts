#!/usr/bin/env python3

"""
An overview of Python's built-in logging module.

Source: https://realpython.com/python-logging/
"""

import logging

# Formatting Output | Set up basic configuration
format = '%(asctime)s - %(process)d - %(levelname)s - %(message)s'
logging.basicConfig(format=format, level=logging.INFO)

# With the logging module imported, you can use something called a “logger”
#   to log messages that you want to see. By default, there are 5 standard
#   levels indicating the severity of events. Each has a corresponding method
#   that can be used to log events at that level of severity. The defined
#   levels, in order of increasing severity, are the following:
logging.debug('I am a debug message')
logging.info('I am some information')
logging.warning('I am a warning message')
logging.error('I am error message')
logging.critical('I am a critical message')

# Logging Variable Data
name = 'John'
logging.error(f'{name} raised an error')

# Capturing Stack Traces Part 1
a = 5
b = 0

try:
    c = a / b
except Exception as e:
    logging.error('Exception occured', exc_info=True)

# Capturing Stack Traces Part 2
# logging.exception() is like calling loggin.error(exc_info=True)
a = 5
b = 0

try:
    c = a / b
except Exception as e:
    logging.exception('Exception occured')
