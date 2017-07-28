# WARNING: this image is PUBLIC, and should not contain any proprietary code.
FROM ruby:2.3

# if you change this file, or need to update package versions, make sure to update the below labels
# rebuilding this image should be done with ./bin/docker-build-circle-image
LABEL repo=getoutreach/etl-circle
LABEL version=1.0

RUN apt-get update &&\
    apt-get install -y python-pip python-dev &&\
    pip install awscli pre-commit &&\
    gem update --system &&\
    gem install bundler &&\
    mkdir empty &&\
    mkdir config &&\
    apt-get clean
