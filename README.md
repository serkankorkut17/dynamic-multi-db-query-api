# dynamic-multi-db-query-api

FETCH(Ad, Soyad) FROM Dataset
FETCH(Ad='Serkan', Soyad) FROM Dataset
FETCH(Age>30, Salary<5000) FROM Employee
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City GROUP Country
FETCH(Ad, Soyad, City.Name, City.Country.Name) FROM Person INCLUDE City.Country

FETCH(Country, COUNT(*)) FILTER((Ad = 'Serkan' OR Grade > 3) AND (Soyad BEGINSWITH 'Ah' AND Email CONTAINS '@' AND Email ENDSWITH '.com' ))

FETCH(Country, COUNT(*)) FROM(Person) GROUPBY(Name) ORDERBY(Name ASC) TAKE(10) LIMIT(10)

# ########################
filterları test et ----- null, not null 
filterlar butun dblerde test et

db output endpointi

expression splitter düzenle
inspect db düzenle

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