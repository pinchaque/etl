set -ex

service postgresql start
sleep 5
su postgres bash -c "psql -c \"CREATE USER root WITH PASSWORD 'password';\""
su postgres bash -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE postgres to root;\""
