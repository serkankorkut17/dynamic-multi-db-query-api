# dynamic-multi-db-query-api

FETCH(Ad, Soyad) FROM Dataset
FETCH(Ad='Serkan', Soyad) FROM Dataset
FETCH(Age>30, Salary<5000) FROM Employee
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City GROUP Country
FETCH(Ad, Soyad, City.Name, City.Country.Name) FROM Person INCLUDE City.Country

FETCH(Ad, Soyad, Email) FILTER(Ad BEGINSWITH('...') AND Soyad CONTAINS('...') AND Email ENDSWITH('...')) FROM Person 

FETCH(Country, COUNT(*)) FROM Person

# POSTGRE
Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=querydb

# SQL SERVER
Server=localhost,1433;Database=master;User ID=sa;Password=Merhaba123.;Encrypt=True;TrustServerCertificate=True

# MYSQL
Server=localhost;Port=3306;Database=querydb;User=mysql;Password=mysql;SslMode=None;AllowPublicKeyRetrieval=True