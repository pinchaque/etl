set -e

export odbc_dir=$HOME/odbc

if [ -d "$HOME/odbc" ]; then
  rm -rdf $HOME/odbc
fi

sudo apt-get update; sudo apt-get install unixodbc-dev

mkdir -p $odbc_dir

wget https://s3-us-west-2.amazonaws.com/outreach-builds/redshift/amazonredshiftodbc-64-bit_1.3.1-2_amd64.deb -O $odbc_dir/amazonredshiftodbc-64-bit_1.3.1-2_amd64.deb

sudo dpkg -i $odbc_dir/amazonredshiftodbc-64-bit_1.3.1-2_amd64.deb


echo "write odbc.ini"
cat << EOF > $odbc_dir/odbc.ini
[ODBC Data Sources]
MyRealRedshift=MyRedshiftDriver

[MyRealRedshift]
# Driver: The location where the ODBC driver is installed to.
Driver=/opt/amazon/redshiftodbc/lib/64/libamazonredshiftodbc64.so

# Required: These values can also be specified in the connection string.
Server=[Server]
Port=[Port]
Database=[Database]
locale=en-US
EOF

echo "write odbcint.ini"
cat << EOF > $odbc_dir/odbcinst.ini
[ODBC Drivers]
Amazon Redshift=Installed

[MyRedshiftDriver]
Description=Amazon Redshift ODBC Driver
Driver=/opt/amazon/redshiftodbc/lib/64/libamazonredshiftodbc64.so
EOF

echo "write amazon.redshiftodbc.ini"
cat << EOF > $odbc_dir/amazon.redshiftodbc.ini
[Driver]
#EG# DriverManagerEncoding=UTF-32
DriverManagerEncoding=UTF-16
ErrorMessagesPath=/opt/amazon/redshiftodbc/ErrorMessages
LogLevel=0
LogPath=[LogPath]

ODBCInstLib=libiodbcinst.so
EOF

echo "write odbc envvarsi"
cat << EOF > $odbc_dir/envvars.sh
export LD_LIBRARY_PATH=/opt/amazon/redshift/lib
export ODBCINI=$HOME/odbc/odbc.ini
export AMAZONREDSHIFTODBCINI=$HOME/odbc/amazon.redshiftodbc.ini
export ODBCSYSINI=$HOME/odbc
EOF
