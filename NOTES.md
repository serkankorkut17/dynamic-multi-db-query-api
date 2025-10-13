# POSTGRESQL
Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=querydb;SSL Mode=Disable;

postgresql://postgres:postgres@localhost:5432/querydb?sslmode=disable

# MSSQL
Server=localhost,1433;Database=master;User Id=sa;Password=Merhaba123.;Encrypt=true;TrustServerCertificate=true;

Server=tcp:localhost,1433;Initial Catalog=master;User ID=sa;Password=Merhaba123.;Encrypt=true;TrustServerCertificate=true;

# MYSQL
Server=localhost;Port=3306;Database=querydb;User=mysql;Password=mysql;SslMode=None;AllowPublicKeyRetrieval=True
mysql://mysql:mysql@localhost:3306/querydb

# ORACLE
User Id=system;Password=oracle;Data Source=localhost/FREEPDB1;
User Id=system;Password=oracle;Data Source=localhost:1521/FREEPDB1;

system/oracle@localhost/FREEPDB1

# MONGODB
mongodb://mongo:mongo@localhost:27017/querydb?authSource=admin
mongodb://mongo:mongo@localhost:27017/?authSource=admin

# API
https://jsonplaceholder.typicode.com/
https://dummyjson.com/