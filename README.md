# dynamic-multi-db-query-api
count, sum, avg, min, max, joins, group by, order by, limit, offset, distinct

FETCH(Ad, Soyad) FROM Dataset
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City GROUPBY (Country)
FETCH(Ad, Soyad, City.Name, City.Country.Name) FROM Person INCLUDE (City.Country)

FETCH(Country, COUNT(*)) FILTER((Ad = 'Serkan' OR Grade > 3) AND (Soyad BEGINSWITH 'Ah' AND Email CONTAINS '@' AND Email ENDSWITH '.com' ))

FETCH(Country, COUNT(*)) FROM(Person) GROUPBY(Name) ORDERBY(Name ASC) TAKE(10) LIMIT(10)

# ########################
filterları test et ----- null, not null != null da en son convert edilebilir
filterlar butun dblerde test et

db output endpointi
1- queryddeki columnları al table.column
2- her column için datatype veren kodu yazarak bunu elde et
3- yoksa tablo oluştur varsa eksikleri alter et nullable olması gerekir tam tersi fazlaysa error belki de
eğer CREATE TABLE ise hepsi nullable olsun
ALTER TABLE ADD COLUMN burda fazlalıklar not nullsa nullable yap
4- tek tek insert yap

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