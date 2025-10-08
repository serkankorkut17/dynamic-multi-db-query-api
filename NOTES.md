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
https://jsonplaceholder.typicode.com/***


conditiona kadar ulaş sonra eğer fonksiyonsa add fieldsa ekleme yaptır
add fields recursive olsun burada işlesin iç içe olanları
yoksa direk conditionu döndirebilirz

aynı fonksiyon fetch kısmı içinde kullanılacak


SELECT COUNT(*) AS total, SUM(salary) AS total_salary, AVG(salary) AS avg_salary
FROM teachers;

var pipeline = new List<BsonDocument>
{
    new BsonDocument("$group", new BsonDocument
    {
        { "_id", BsonNull.Value },  // Grup yok, tüm tablo tek grup
        { "total", new BsonDocument("$sum", 1) },
        { "total_salary", new BsonDocument("$sum", "$salary") },
        { "avg_salary", new BsonDocument("$avg", "$salary") }
    })
};



SELECT school_id, COUNT(*) AS total, AVG(salary) AS avg_salary
FROM teachers
GROUP BY school_id;


var pipeline = new List<BsonDocument>
{
    new BsonDocument("$group", new BsonDocument
    {
        { "_id", "$school_id" },   // GROUP BY
        { "total", new BsonDocument("$sum", 1) },
        { "avg_salary", new BsonDocument("$avg", "$salary") }
    }),
    new BsonDocument("$project", new BsonDocument
    {
        { "school_id", "$_id" },
        { "total", 1 },
        { "avg_salary", 1 },
        { "_id", 0 }
    })
};



***** mongo db tablo gösterme
mongodb kaydetme