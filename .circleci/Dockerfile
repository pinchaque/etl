# WARNING: this image is PUBLIC, and should not contain any proprietary code.
FROM ruby:2.3

# if you change this file, or need to update package versions, make sure to update the below labels
# rebuilding this image should be done with ./bin/docker-build-circle-image
LABEL repo=getoutreach/etl-circle
LABEL version=1.1

RUN apt-get update &&\
    apt-get install -y python-pip python-dev unixodbc-dev&&\
    pip install awscli pre-commit &&\
    gem update --system &&\
    gem install bundler &&\
    mkdir ~/empty &&\
    mkdir ~/config &&\
    mkdir ~/odbc &&\
    wget https://s3-us-west-2.amazonaws.com/outreach-builds/redshift/amazonredshiftodbc-64-bit_1.3.1-2_amd64.deb \
        -O ~/odbc/amazonredshiftodbc-64-bit_1.3.1-2_amd64.deb &&\
    dpkg -i ~/odbc/amazonredshiftodbc-64-bit_1.3.1-2_amd64.deb &&\
    apt-get clean
