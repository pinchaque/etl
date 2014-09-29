#!/bin/bash
# Script to set up postgres database

su -l postgres -c "psql -c \"create role dw with login createdb password 'toOlY7oLVFCOL4XudzmJ'\""

su -l csmith cd ~/git/etl && rake db:create
