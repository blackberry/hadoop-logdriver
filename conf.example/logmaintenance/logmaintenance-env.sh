## The directory where the log maintenance tool will write its logs.
export LOGMAINTENANCE_LOG_DIR=/var/log/logmaintenance

## How long would you like to keep the logs for?
export DELETE_LOGS_AFTER_DAYS=7


## Number of concurrent merge jobs to be run
## 0 means unlimited
export MAX_CONCURRENT_JOBS=1


## Because the logmaintenance script needs to change kerberos principals,
## you must set the path to the kinit command.
export KINIT=/usr/kerberos/bin/kinit


## If you would like to receive emails when there is an error, include an
## email address, and the path to the sendmail command.  Leaving the email
## address blank will turn off error reporting.
export EMAIL_ADDRESS=admin@example.com
export SENDMAIL=/usr/sbin/sendmail
