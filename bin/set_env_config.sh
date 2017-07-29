#!/usr/bin/env bash
job_dir=~/etl_test_config
class_dir=~/etl_test_config

redshift_password=$(echo ${REDSHIFT_PASSWORD})
database_password=$(echo ${DATABASE_PASSWORD})
influxdb_password=$(echo ${INFLUXDB_PASSWORD})

export ETL_CORE_ENVVARS=true
export ETL_CLASS_DIR=${class_dir}
export ETL_DATA_DIR=/var/tmp/etl_test_output

# leaving empty for tests, another option is to 
# have another channel for testing to see when they fail?
export ETL_SLACK_URL=
export ETL_SLACK_CHANNEL=
export ETL_SLACK_USERNAME=

# database replacement for core and database.yml
export ETL_DATABASE_ENVVARS=true
export ETL_DATABASE_DB_NAME=postgres
export ETL_DATABASE_USER=postgres
export ETL_DATABASE_PASSWORD=2408unvvgv34
export ETL_DATABASE_HOST=localhost
export ETL_DATABASE_PORT=5432

# aws.yml replacement
export ETL_AWS_ENVVARS=true
export ETL_AWS_REGION=us-west-1
export ETL_AWS_S3_BUCKET=ss-uw1-stg.redshift-testing
export ETL_AWS_ROLE_ARN="arn:aws:iam::182192988802:role/ss-uw1-stg-default-redshift-testing"

# redshift.yml replacement
export ETL_REDSHIFT_ENVVARS=true
export ETL_REDSHIFT_DB_NAME=dev
export ETL_REDSHIFT_USER=masteruser
export ETL_REDSHIFT_PASSWORD=$REDSHIFT_PASSWORD
export ETL_REDSHIFT_HOST=dw-testing.outreach-staging.com
export ETL_REDSHIFT_PORT=5439

#ETL_INFLUX_ENVVARS

