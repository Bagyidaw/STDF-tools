This perl script parses and loads STDF records into RDBMS like sqlite3 or postgresql or any other database supported by perl DBI module.
From there one could use SQL/R/python to do data analysis/charting.

You must have perl module STDF::Parser installed and require perl version 5.12 and above.

perl stdfloader.pl   < stdf file path>   < stdf_database.db>

perl stdfloader.pl   <stdf file path>  < database name>  # could be postgresql databasename 
