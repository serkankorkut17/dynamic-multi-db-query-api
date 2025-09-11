# dynamic-multi-db-query-api

FETCH(Ad, Soyad) FROM Dataset
FETCH(Ad='Serkan', Soyad) FROM Dataset
FETCH(Age>30, Salary<5000) FROM Employee
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City
FETCH(Ad, Soyad, City.Name) FROM Person INCLUDE City GROUP Country
FETCH(Ad, Soyad, City.Name, City.Country.Name) FROM Person INCLUDE City.Country

FETCH(Country, COUNT(*, grade))FILTER((Ad = 'Serkan' OR Grade > 3) AND (Soyad BEGINSWITH 'Ah' AND Email CONTA
IFETCH(Country, COUNT(*)) FROM(Person) GROUPBY(Name) ORDERBY(Name ASC) TAKE(10) LIMIT(10)
NS '@' AND Email ENDSWITH '.com' ))gres;Database=querydb

# ########################
**having
query parser düzenle
sql query builder oluşturulabilir
expression splitter düzenle

**test
*test multi db