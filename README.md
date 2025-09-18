# Dynamic DB Query

## SQL vs My Query Language
|                |SQL                          |MY QUERY                        |
|------------|----------------------|-----------------------------|
|SELECT|   SELECT name, surname     |FETCH (name, surname)  |
|DISTINCT|   SELECT DISTINCT   |FETCHD / FETCH DISTINCT(...) |
|FROM  |FROM table            |FROM table / FROM (table)|
|JOIN  |[joinType] JOIN table2 ...|INCLUDE (table2)|
|JOIN CHAINING |... JOIN table3 ...|INCLUDE (table2.table3)|
|SELECT (JOIN) |SELECT table2.id| FETCH (name, surname, table2.id)|
|WHERE |WHERE age >= 18 | FILTER (age >= 18)|
|GROUP BY |GROUP BY name | GROUPBY (name)|
|HAVING |HAVING COUNT(*) >= 20 | HAVING (COUNT(*) >= 20)|
|ORDER BY |ORDER BY name DESC | ORDERBY (name DESC)|
|LIMIT |LIMIT 10 | TAKE (10) / LIMIT (10)|
|OFFSET |OFFSET 10 | SKIP (10) / OFFSET (10)|

My Query Example:
```sql
FETCH(City.Country.Name AS country_name, COUNT(*) AS total)
FROM(person)
INCLUDE(City.Country)
FILTER((name = 'Serkan' OR grade > 3) AND (surname BEGINSWITH 'Ah' AND email CONTAINS '@' AND email ENDSWITH '.com'))
GROUPBY(City.Country.Name)
HAVING(COUNT(*) > 2)
ORDERBY(total DESC)
TAKE(10)
```
PostgreSQL:
```sql
SELECT co.name AS country_name, COUNT(*) AS total
FROM person p
LEFT JOIN city c ON c.id = p.city_id
LEFT JOIN country co ON co.id = c.country_id
WHERE (p.name =  'Serkan'  OR p.grade >  3)
AND (p.surname LIKE  'Ah%'  AND p.email LIKE  '%@%'  AND p.email LIKE  '%.com')
GROUP BY co.name
HAVING  COUNT(*) >  2
ORDER BY total DESC
LIMIT  10;
```

## Supported Operators

### 1. Comparison Operators

|OPERATOR                       |SQL EQUIVALENT             |
|-------------------------------|---------------------------|
|= / ==													|   =     							  	|
|!= / <>												|   <>  									  |
|>															|   >  										   |
|<															|   < 											 |
|>=															|   >= 										  |
|<=															|   <=											|

  
### 2. String Match Operators

|OPERATOR                       |SQL EQUIVALENT             |
|-------------------------------|---------------------------|
|CONTAINS												|   col LIKE '%val%'  	  	|
|BEGINSWITH											|   col LIKE 'val%'  			  |
|ENDSWITH												|   col LIKE '%val'  		   |
|LIKE														|   col LIKE 'same'				|

  
### 3. NULL Operators

|OPERATOR                       |SQL EQUIVALENT             |
|-------------------------------|---------------------------|
|col = NULL											|   col IS NULL  	  	|
|col == NULL										|   col IS NULL  			  |
|col != NULL										|   col IS NOT NULL  		   |
|col <> NULL										|   col IS NOT NULL				|
  

### 4. Logical Operators & Precedence

|OPERATOR                       |PRECEDENCE             |
|-------------------------------|---------------------------|
|(...)													|   1 (Highest)  					 |
|AND														|   2 (High) 	  					|
|OR															|   3 (Low)  			 			 |

Example: FILTER(a = 1 OR b = 2 AND c = 3)
Internal: a = 1 OR (b = 2 AND c = 3)


## Supported Functions

### 1. Aggregate Functions

| Function    | Example    | Description | Generated SQL|
|-------------|------------|-------------|--------------|
| COUNT(expr) | COUNT(*).  | Row count   | COUNT(*)     |
| SUM(col)    | SUM(Price) | Sum         | SUM(Price)   |
| AVG(col)    | AVG(Grade) | Average     | AVG(Grade)   |
| MIN(col)    | MIN(Grade) | Minimum     | MIN(Grade)   |
| MAX(col)    | MAX(Grade) | Maximum     | MAX(Grade)   |


### 2. Numeric Functions

| Function               | Example        | Description        |
|------------------------|----------------|--------------------|
| ABS(x)                 | ABS(-5)        | Absolute value     |
| CEIL(x) / CEILING(x)   | CEIL(4.2)      | Round up           |
| FLOOR(x)               | FLOOR(4.8)     | Round down         |
| ROUND(x, p) / ROUND(x) | ROUND(Grade,2) | Round to precision |
| SQRT(x)                | SQRT(9)        | Square root        |
| POWER(x,y)             | POWER(2,8)     | Exponent           |
| MOD(x,y)               | MOD(a,b)       | Modulus            |
| EXP(x)                 | EXP(1)         | e^x                |
| LOG(x)                 | LOG(100)       | Natural log        |
| LOG(x,base)            | LOG(100,10)    | Log with base      |
| LOG10(x)               | LOG10(100)     | Base-10 log        |


### 3. String Functions

| Function               | Example         | Description        |
|------------------------|-----------------|--------------------|
| LENGTH(col) / LEN(col) | LENGTH(Name)    | Length             |
| SUBSTRING(str,start,len) / SUBSTR | SUBSTRING(Name,0,3) | Slice (0-based) |
| CONCAT(a,b,...)        | CONCAT(First,Last) | Concatenate     |
| LOWER(col)             | LOWER(Name)      | To lower          |
| UPPER(col)             | UPPER(Name)      | To upper          |
| TRIM/LTRIM/RTRIM(col)  | TRIM(Name)       | Whitespace trim   |
| INDEXOF(str,search)    | INDEXOF(Email,'@') | 0-based index   |
| REPLACE(str,old,new)   | REPLACE(Name,'a','x') | Replace substring |
| REVERSE(str)           | REVERSE(Code)    | Reverse string    |


### 4. Null Functions

| Function               | Example          | Description       |
|------------------------|------------------|-------------------|
| COALESCE(a,b,...)      | COALESCE(Email,'-') | First non-null |
| IFNULL(a,b)            | IFNULL(Phone,'-') | Alias            |
| ISNULL(a,b)            | ISNULL(Phone,'-') | Alias            |
| NVL(a,b)               | NVL(Phone,'-')    | Alias            |


### 5. Date / Time Functions

| Function               | Example          | Description           |
|------------------------|------------------|-----------------------|
| NOW() / GETDATE() / CURRENT_TIMESTAMP | NOW() | Current timestamp |
| CURRENT_DATE           | CURRENT_DATE     | Current date          |
| CURRENT_TIME           | CURRENT_TIME     | Current time          |
| DATEADD(unit,date,n)   | DATEADD(DAY,Date,5) | Add interval       |
| DATEDIFF(unit,start,end) | DATEDIFF(DAY,Start,End) | Difference   |
| DAY(date)              | DAY(CreatedAt)   | Day part              |
| MONTH(date)            | MONTH(CreatedAt) | Month part            |
| YEAR(date)             | YEAR(CreatedAt)  | Year part             | 
| DATENAME(unit,date)    | DATENAME(MONTH,CreatedAt) | Name of part |

- DATEADD: unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
> DATEADD(HOUR,'2025-09-18 12:34:56',2) => 2025-09-18 14:34:56

- DATEDIFF: unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
> DATEDIFF(DAY,'2025-09-03','2025-09-18') => 15

- DATENAME: unit can be MONTH or DAY
> DATENAME(MONTH, '2025-09-18') => 'September'

### Notes
- SUBSTRING/SUBSTR start index is 0 in my Query Language; adjusted to 1-based in SQL.
- INDEXOF returns 0-based by subtracting 1 from function result.
- LOG(x, base) emulation on SQL Server uses division of natural logs.
- CONCAT applies NULL safety on MySQL via COALESCE; Oracle uses || and leaves NULLs (standard Oracle concatenation).
- Boolean literal adaptations occur inside [`SqlBuilderService.ConvertFilterToSql`](DynamicDbQueryApi/Services/SqlBuilderService.cs) (e.g., PostgreSQL TRUE/FALSE, MySQL 1/0, Oracle 1/0, SQL Server 1/0).
- Nested function arguments are recursively resolved.


### 6. DSL → SQL Example
My Query Example:
```sql
FETCH(CONCAT(LOWER(p.Name), '-', YEAR(p.CreatedAt)) AS slug, COUNT(*) AS total)
FROM Person p
GROUPBY(slug)
ORDERBY(total DESC)
```
PostgreSQL:
```sql
SELECT CONCAT(LOWER(p.Name), '-', EXTRACT(YEAR FROM p.CreatedAt)) AS slug, COUNT(*) AS total
FROM Person p
GROUP BY slug
ORDER BY total DESC
```