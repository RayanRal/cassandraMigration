### Usage
`CassandraMigrationTool` helps to migrate Cassandra DB tables.


### How to run the app
App can be run from command line using request as:

    $ java -Dconfig=src/main/resources/server.conf -Ddbschema=src/main/resources/dbschema.json -cp DbMigrationTool-assembly-${version}.jar com.gmail.migrationtool.MigrationTool <import/export> <keyspace> <table>

System properties:

config - File with connection parameters, such as ip address of Db, port and so on. If parameter is not specified, then `dev-server.conf` will be used. `fetchSize` parameter - Fetch size for export, should be specified for 'heavy' tables, which fail export in case of default value. If you're not sure, what value to set - try setting to 0, so it will be chosen by Cassandra.

dbschema - File with schema of table, that will be exported/imported. Export: If schema is not specified, all columns would be exported. Import: if schema is not specified, tool will try to insert all values from .csv file. Array "blobColumns" is needed for cases when column is used for storing blob, but has other type (for example, text). If column is of type BLOB, then it's not necessary to specify it explicitly.


### Export logic

Columns, specified in `dbschema` file (or all columns, if file is empty) will be exported to a folder, named same as table name. Each row would be saved to a separate folder, folder name is concatenation of all primary keys of the table, filename for data - 'data.csv'. Columns of type BLOB wouldbe exported to a separate file, along with columns, specified in `blobColumns` section of dbschema file (filename equals to name of the column). You need to specify only columns, that are used for storing BLOBs, while having other type (e.g. "encoded_image" in `enrolments` table).


### Import logic

Column, specified in `dbschema` file (or all columns, if file is empty) will be imported from a folder, named same as table name.
