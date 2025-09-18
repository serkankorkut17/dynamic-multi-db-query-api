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

|MY QUERY                       |SQL EQUIVALENT             |
|-------------------------------|---------------------------|
|= / ==													|   =     							  	|
|!= / <>												|   <>  									  |
|>															|   >  										   |
|<															|   < 											 |
|>=															|   >= 										  |
|<=															|   <=											|

  
### 2. String Match Operators

|MY QUERY                       |SQL EQUIVALENT             |
|-------------------------------|---------------------------|
|CONTAINS												|   col LIKE '%val%'  	  	|
|BEGINSWITH											|   col LIKE 'val%'  			  |
|ENDSWITH												|   col LIKE '%val'  		   |
|LIKE														|   col LIKE 'same'				|

  
### 3. NULL Operators

|MY QUERY                       |SQL EQUIVALENT             |
|-------------------------------|---------------------------|
|col = NULL											|   col IS NULL  	  	|
|col == NULL										|   col IS NULL  			  |
|col != NULL										|   col IS NOT NULL  		   |
|col <> NULL										|   col IS NOT NULL				|
  

### 4. Logical Operators & Precedence

|OPERATORS                      |PRECEDENCE             |
|-------------------------------|---------------------------|
|(...)													|   1 (Highest)  					 |
|AND														|   2 (High) 	  					|
|OR															|   3 (Low)  			 			 |

Example: FILTER(a = 1 OR b = 2 AND c = 3)
Internal: a = 1 OR (b = 2 AND c = 3)


## Supported Functions

### 1. Aggregate Functions



### 2. Numeric Functions



### 3. String Functions



### 4. Null Functions