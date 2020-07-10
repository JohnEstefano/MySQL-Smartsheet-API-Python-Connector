## MySQL - Smartsheet API - Python - Connector
Python application that interacts with both a mySql database and Smartsheet API.\
Very specific usecase that takes data from database, modifies it in a very specific way then updates specified Smartsheet sheet.


### Dev System Environment
Ubuntu 18.04.1 LTS\
Python 3.7.4\
mysql-connector-python 8.0.20\
pandas 0.25.1\
smartsheet-python-sdk 2.86.0


### Required Python Libraries: Installation (run in shell)
`pip install mysql-connector-python`\
`pip install pandas`\
`pip install smartsheet-python-sdk`


### How to use
First, modify both the `credentials_inputs_template.py` and `main.py` files to fit your specific needs.\
Second, rename the modified `credentials_inputs_template.py` to `credentials_inputs.py`.\
Third, place `credentials_inputs.py` and `main.py` in same directory.\
Lastly, Run `main.py`.

#### Run as a scheduled task
On Linux, the Linux crontab could be used for running specific tasks on a regular interval.\
On Windows, the windows task schedules could be used for running specific tasks on a regular interval.


### Notes
The application creates a logfile called `log.log` within the same directory as the `main.py` script.
It collects any errors that may happen during critical points of failure, like for instance, not being able to connect to the database. It also collects Smartsheet API requests, and logs whether or not a specified row is successfully injected into the smartsheet sheet.


### API reference
The Smartsheet API reference is available at:\
https://smartsheet-platform.github.io/api-docs/
