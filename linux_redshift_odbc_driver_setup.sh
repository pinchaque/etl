set -e

export odbc_dir=$HOME/odbc

echo "write odbc.ini"
cat << EOF > $odbc_dir/odbc.ini
[ODBC Data Sources]
MyRealRedshift=Amazon Redshift (x64)

[MyRealRedshift]
# Driver: The location where the ODBC driver is installed to.
Driver=/opt/amazon/redshiftodbc/lib/64/libamazonredshiftodbc64.so

# Required: These values can also be specified in the connection string.
Server=dw-testing.outreach-staging.com
Port=5439
Database=dev
locale=en-US
EOF

echo "write odbcint.ini"
cat << EOF > $odbc_dir/odbcinst.ini
[ODBC Drivers]
Amazon Redshift=Installed

[Amazon Redshift (x64)]
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

echo "write odbc envvars.sh"
cat << EOF > $odbc_dir/envvars.sh
export LD_LIBRARY_PATH=/opt/amazon/redshift/lib
export ODBCINI=$HOME/odbc/odbc.ini
export AMAZONREDSHIFTODBCINI=$HOME/odbc/amazon.redshiftodbc.ini
export ODBCSYSINI=$HOME/odbc
EOF
