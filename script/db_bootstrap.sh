#!/bin/bash
# Script to set up postgres database

su -l postgres -c "psql -c \"create role dw with login createdb password 'toOlY7oLVFCOL4XudzmJ'\""

su -l csmith cd ~/git/etl && rake db:create

# set up mysql for testing
mysql -u root -e "create user 'dw'@'localhost' identified by 'toOlY7oLVFCOL4XudzmJ'"
mysql -u root -e "grant all privileges on * . * to 'dw'@'localhost'"
mysql -u root -e "flush privileges"
