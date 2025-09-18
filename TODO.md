# dynamic-multi-db-query-api
count, sum, avg, min, max, joins, group by, order by, limit, offset, distinct

contains(...) oluyo mu bak !!!!!

eklenecek:
LENGTH(col) - LEN(col)
SUBSTRING(col, start, len) - SUBSTR(col, start, len)
CONCAT(col1, col2)

FETCH(Ad, Soyad) FROM Dataset
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City GROUPBY (Country)
FETCH(Ad, Soyad, City.Name, City.Country.Name) FROM Person INCLUDE (City.Country)

FETCH(Country, COUNT(*)) FILTER((Ad = 'Serkan' OR Grade > 3) AND (Soyad BEGINSWITH 'Ah' AND Email CONTAINS '@' AND Email ENDSWITH '.com' ))

FETCH(Country, COUNT(*) AS count) FROM(Person) GROUPBY(Name) HAVING(count(*) > 2) ORDERBY(Name ASC) TAKE(10) LIMIT(10)

queryleri at uygun mudur diye sor
as vb sorulabilir 

# ########################
***filterları test et ----- null, not null != null da en son convert edilebilir
***filterlar butun dblerde test et

**** sql builder iç içe fonksiyon kontrolu recursive
**** alias isimleri düzelt fonksiyonlar için

expression splitter düzenle

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