set -e

export odbc_dir=$HOME/odbc

echo $odbc_dir
if [ -d "$HOME/odbc" ]; then
  rm -rdf $HOME/odbc
fi

mkdir -p $odbc_dir

wget https://s3.amazonaws.com/redshift-downloads/drivers/AmazonRedshiftODBC-1.3.1.1000.dmg -O $odbc_dir/AmazonRedshiftODBC-1.3.1.1000.dmg

sudo hdiutil attach $odbc_dir/AmazonRedshiftODBC-1.3.1.1000.dmg
sudo installer -package /Volumes/AmazonRedshiftODBC-1.3.1.1000/AmazonRedshiftODBC-1.3.1.1000.pkg -target /
sudo hdiutil detach /Volumes/AmazonRedshiftODBC-1.3.1.1000

echo "write odbc.ini"
cat << EOF > $odbc_dir/odbc.ini
[ODBC Data Sources]
MyRealRedshift=MyRedshiftDriver

[MyRealRedshift]
# Driver: The location where the ODBC driver is installed to.
Driver=/opt/amazon/redshift/lib/libamazonredshiftodbc.dylib

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
Driver=/opt/amazon/redshift/lib/libamazonredshiftodbc.dylib
EOF

echo "write amazon.redshiftodbc.ini"
cat << EOF > $odbc_dir/amazon.redshiftodbc.ini
[Driver]
#EG# DriverManagerEncoding=UTF-32
DriverManagerEncoding=UTF-16
ErrorMessagesPath=/opt/amazon/redshiftodbc/ErrorMessages
LogLevel=0
LogPath=[LogPath]

ODBCInstLib=libiodbcinst.dylib
EOF

echo "write odbc envvars.sh"
cat << EOF > $odbc_dir/envvars.sh
export DYLD_LIBRARY_PATH=/opt/amazon/redshift/lib
export ODBCINI=$HOME/odbc/odbc.ini
export AMAZONREDSHIFTODBCINI=$HOME/odbc/amazon.redshiftodbc.ini
export ODBCSYSINI=$HOME/odbc
EOF

brew install unixodbc

echo "-----------------------"
echo "IMPORTANT"
echo "$odbc_dir/envvars.sh needs to be sourced to enable redshift odbc driver to work"
echo "~~~~~~~~~~~~~~~~~~~~~~~"

