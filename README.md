# Dynamic DB Query

## Overview Dynamic DB Query is a .NET Web API that:

- Parses a custom query DSL into dialect-specific SQL
- Executes queries against Postgres, MySQL, SQL Server, and Oracle
- Inspects database schemas (tables, columns, relationships)
- Optionally writes query results into an output database
- DSL → SQL builder with JOINs, filters, grouping, ordering, and pagination
- Raw SQL execution endpoint
- Schema inspection endpoint

## Supported databases

- DbType values: postgres | postgresql | mysql | mssql | oracle

## API Endpoints

### POST /api/query

Purpose: Execute a DSL query (the custom “My Query Language”).<br>
Request body (QueryRequestDTO):

```json
{
	"DbType": "postgres",
	"ConnectionString": "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=querydb;SSL Mode=Disable;",
	"Query": "FETCH(courses.name, COUNT(enrollments.id) AS enroll_cnt) FROM courses INCLUDE(enrollments) GROUPBY(courses.name) HAVING(enroll_cnt > 5) ORDERBY(enroll_cnt DESC)",
	"WriteToOutputDb": true, // Optional, default is false
	"OutputDbType": "mysql", // Required if WriteToOutputDb is true
	"OutputDbType": "oracle", // Required if WriteToOutputDb is true
	"OutputConnectionString": "User Id=system;Password=oracle;Data Source=localhost/FREEPDB1;", // Required if WriteToOutputDb is true
	"OutputTableName": "result_table" // Required if WriteToOutputDb is true
}
```

Response body (QueryResultDTO):

```json
{
	"sql": "SELECT courses.name, COUNT(enrollments.id) AS enroll_cnt FROM courses LEFT JOIN enrollments ON enrollments.course_id = c.id GROUP BY courses.name HAVING COUNT(enrollments.id) > 5 ORDER BY enroll_cnt DESC",
	"data": [
		{ "name": "Algebra", "enroll_cnt": 12 },
		{ "name": "Physics", "enroll_cnt": 8 }
	],
	"writtenToOutputDb": true
}
```

### POST /api/query/sql

Purpose: Execute raw SQL as-is.<br>
Request body (QueryRequestDTO), same shape as above; Query contains SQL.<br>
Response body (QueryResultDTO), same shape as above.

### POST /api/query/inspect

Purpose: Inspect schema: tables, columns, and relationships in the input DB.<br>
Request body (InspectRequestDTO):

```json
{
	"DbType": "postgres",
	"ConnectionString": "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=querydb;SSL Mode=Disable;"
}
```

Response body (InspectResponseDTO):

```json
{
  "tables": [
    {
      "table": "schools",
      "columns": [
        { "name": "id", "dataType": "integer", "isNullable": false },
        { "name": "name", "dataType": "text", "isNullable": false },
        { "name": "city", "dataType": "text", "isNullable": false },
        { "name": "is_public", "dataType": "boolean", "isNullable": false },
        { "name": "established_year", "dataType": "integer", "isNullable": true },
        { "name": "metadata", "dataType": "jsonb", "isNullable": true },
        { "name": "created_at", "dataType": "timestamp with time zone", "isNullable": false }
      ],
      "relationships": [
        { "key": "id", "relationTable": "teachers", "relationKey": "school_id" },
        { "key": "id", "relationTable": "classes", "relationKey": "school_id" },
        { "key": "id", "relationTable": "courses", "relationKey": "school_id" }
      ]
    },
    ... more tables ...
  ],
  "relationships": [
    { "fkTable": "teachers", "fkColumn": "school_id", "pkTable": "schools", "pkColumn": "id" },
    ... more relationships ...
  ]
}
```

## SQL vs My Query Language

|                | SQL                        | MY QUERY                                 |
| -------------- | -------------------------- | ---------------------------------------- |
| SELECT         | SELECT name, surname       | FETCH (name, surname)                    |
| DISTINCT       | SELECT DISTINCT            | FETCHD / FETCH DISTINCT(...)             |
| FROM           | FROM table                 | FROM table / FROM (table)                |
| JOIN           | [joinType] JOIN table2 ... | INCLUDE (table2) (left join)             |
| INNER JOIN     | INNER JOIN table2 ...      | INCLUDE (table2 INNER)                   |
| JOIN CHAINING  | ... JOIN table3 ...        | INCLUDE (table2.table3)                  |
| JOIN with keys | ... JOIN table3 ...        | INCLUDE (table2(table1.col, table2.col)) |
| SELECT (JOIN)  | SELECT table2.id           | FETCH (name, surname, table2.id)         |
| WHERE          | WHERE age >= 18            | FILTER (age >= 18)                       |
| GROUP BY       | GROUP BY name              | GROUPBY (name)                           |
| HAVING         | HAVING COUNT(\*) >= 20     | HAVING (COUNT(\*) >= 20)                 |
| ORDER BY       | ORDER BY name DESC         | ORDERBY (name DESC)                      |
| LIMIT          | LIMIT 10                   | TAKE (10) / LIMIT (10)                   |
| OFFSET         | OFFSET 10                  | SKIP (10) / OFFSET (10)                  |

My Query Example:

```sql
FETCH(City.Country.Name AS country_name, COUNT(*) AS total)
FROM(person)
INCLUDE(City.Country)
FILTER((name = 'Serkan' OR grade > 3) AND (surname BEGINSWITH 'Ah' AND email CONTAINS '@' AND email ENDSWITH '.com'))
GROUPBY(country_name)
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

## Aliases

- Define column aliases in FETCH: `FETCH(SUM(price) AS total, ...)`.
- In the query you can reference aliases in FILTER, GROUPBY, HAVING, and ORDERBY.
- Table aliases cannot be manually set.

## JOINs

- Use `INCLUDE(table2)` for JOINs. Default is LEFT JOIN.
- For INNER JOIN use `INCLUDE(table2 INNER)`.
- For RIGHT JOIN use `INCLUDE(table2 RIGHT)`.
- For FULL JOIN use `INCLUDE(table2 FULL)`.
- For JOIN chaining use dot notation: `INCLUDE(table2.table3)`.
- For JOIN with custom keys use: `INCLUDE(table2(table1.col, table2.col))`.

## Arithmetic Operators

- Not supported for now.

## Supported Operators

### 1. Comparison Operators

| OPERATOR    | SQL EQUIVALENT      |
| ----------- | ------------------- |
| = / ==      | =                   |
| != / <>     | <>                  |
| >           | >                   |
| <           | <                   |
| >=          | >=                  |
| <=          | <=                  |
| IN          | IN (...)            |
| NOT IN      | NOT IN (...)        |
| BETWEEN     | BETWEEN ... AND     |
| NOT BETWEEN | NOT BETWEEN ... AND |

- Usage: column OPERATOR value, column OPERATOR(value1, value2, ...)
  > age >= 18
  > <br>
  > age IN(18,21,25)
  > <br>
  > age BETWEEN(18,30)
  > <br>
  > LOWER(name) = 'serkan'

### 2. String Match Operators

| OPERATOR   | SQL EQUIVALENT   |
| ---------- | ---------------- |
| CONTAINS   | col LIKE '%val%' |
| STARTSWITH | col LIKE 'val%'  |
| BEGINSWITH | col LIKE 'val%'  |
| ENDSWITH   | col LIKE '%val'  |
| LIKE       | col LIKE 'same'  |

- Usage: column OPERATOR 'value', column OPERATOR('...')
  > name CONTAINS('Serkan')
  > <br>
  > LOWER(name) CONTAINS('serkan')

### 3. NULL Operators

| OPERATOR        | SQL EQUIVALENT  |
| --------------- | --------------- |
| col = NULL      | col IS NULL     |
| col == NULL     | col IS NULL     |
| col != NULL     | col IS NOT NULL |
| col <> NULL     | col IS NOT NULL |
| col IS NULL     | col IS NULL     |
| col IS NOT NULL | col IS NOT NULL |

### 4. Logical Operators & Precedence

| OPERATOR | PRECEDENCE  |
| -------- | ----------- |
| (...)    | 1 (Highest) |
| AND      | 2 (High)    |
| OR       | 3 (Low)     |

Example: FILTER(a = 1 OR b = 2 AND c = 3)
<br>
Internal: a = 1 OR (b = 2 AND c = 3)

## Supported Functions

### 1. Aggregate Functions

| Function    | Example    | Description |
| ----------- | ---------- | ----------- |
| COUNT(expr) | COUNT(\*)  | Row count   |
| SUM(col)    | SUM(Price) | Sum         |
| AVG(col)    | AVG(Grade) | Average     |
| MIN(col)    | MIN(Grade) | Minimum     |
| MAX(col)    | MAX(Grade) | Maximum     |

### 2. Conditional Functions

| Function                                     | Example                                       | Description             |
| -------------------------------------------- | --------------------------------------------- | ----------------------- |
| IF(condition, true_val, false_val)           | IF(grade >= 90, 'A', 'B')                     | Classic If - Else       |
| IFS(cond1, val1, cond2, val2, ..., elseVal)  | IFS(grade >= 90, 'A', grade >= 80, 'B', 'C')  | If, Else If, ... , Else |
| CASE(cond1, val1, cond2, val2, ..., elseVal) | CASE(grade >= 90, 'A', grade >= 80, 'B', 'C') | Switch - Case           |

- IF(condition, true_val, false_val)
- CASE/IFS(cond1, val1, cond2, val2, ..., elseVal) → SQL: CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ... ELSE elseVal END

Example:

```sql
FETCH(
  CASE(grade >= 90, 'A', grade >= 80, 'B', 'C') AS letter
)
```

### 3. Numeric Functions

| Function               | Example        | Description        |
| ---------------------- | -------------- | ------------------ |
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

### 4. String Functions

| Function                          | Example               | Description       |
| --------------------------------- | --------------------- | ----------------- |
| LENGTH(col) / LEN(col)            | LENGTH(Name)          | Length            |
| SUBSTRING(str,start,len) / SUBSTR | SUBSTRING(Name,0,3)   | Slice (0-based)   |
| SUBSTRING(str,start) / SUBSTR     | SUBSTRING(Name,0)     | Slice (0-based)   |
| CONCAT(a,b,...)                   | CONCAT(First,Last)    | Concatenate       |
| LOWER(col)                        | LOWER(Name)           | To lower          |
| UPPER(col)                        | UPPER(Name)           | To upper          |
| TRIM/LTRIM/RTRIM(col)             | TRIM(Name)            | Whitespace trim   |
| INDEXOF(str,search)               | INDEXOF(Email,'@')    | 0-based index     |
| REPLACE(str,old,new)              | REPLACE(Name,'a','x') | Replace substring |
| REVERSE(str)                      | REVERSE(Code)         | Reverse string    |

### 5. Null Functions

| Function           | Example             | Description    |
| ------------------ | ------------------- | -------------- |
| COALESCE(a,b, ...) | COALESCE(Email,'-') | First non-null |
| IFNULL(a,b, ...)   | IFNULL(Phone,'-')   | Alias          |
| ISNULL(a,b, ...)   | ISNULL(Phone,'-')   | Alias          |
| NVL(a,b, ...)      | NVL(Phone,'-')      | Alias          |

### 6. Date / Time Functions

| Function                 | Example                   | Description                      |
| ------------------------ | ------------------------- | -------------------------------- |
| NOW()                    | NOW()                     | Current timestamp                |
| GETDATE()                | GETDATE()                 | Current timestamp                |
| CURRENT_TIMESTAMP()      | CURRENT_TIMESTAMP()       | Current timestamp                |
| NOW(timezone)            | NOW('Istanbul')           | Current timestamp with time zone |
| TODAY()                  | TODAY()                   | Current date                     |
| CURRENT_DATE()           | CURRENT_DATE()            | Current date                     |
| TODAY(timezone)          | TODAY('Istanbul')         | Current date with time zone      |
| TIME()                   | TIME()                    | Current time                     |
| CURRENT_TIME()           | CURRENT_TIME()            | Current time                     |
| DATEADD(unit,date,n)     | DATEADD(DAY,Date,5)       | Add interval                     |
| DATEDIFF(unit,start,end) | DATEDIFF(DAY,Start,End)   | Difference                       |
| DAY(date)                | DAY(CreatedAt)            | Day part                         |
| MONTH(date)              | MONTH(CreatedAt)          | Month part                       |
| YEAR(date)               | YEAR(CreatedAt)           | Year part                        |
| DATENAME(unit,date)      | DATENAME(MONTH,CreatedAt) | Name of part                     |

- For POSTGRESQL, MYSQL and ORACLE databases timezone should be IANA timezone.

  > Examples: 'Europe/Istanbul', 'America/New_York', 'Asia/Tokyo', 'UTC', 'Etc/GMT+3', 'Etc/GMT-2', etc.

- For SQL SERVER database timezone should be Windows timezone.

  > Examples: 'Turkey Standard Time', 'Eastern Standard Time', 'Tokyo Standard Time', 'UTC', 'GMT Standard Time', etc.

- NOW() / GETDATE() / CURRENT_TIMESTAMP() => returns timestamp without time zone (UTC)

  > `NOW()` => "2025-09-19T22:37:44"

- NOW(timezone) / GETDATE(timezone) / CURRENT_TIMESTAMP(timezone) => returns timestamp in specified timezone

  > `NOW('Europe/Istanbul')` => "2025-09-20T01:37:44" (UTC+3)

- TODAY() / CURRENT_DATE() => returns date (YYYY-MM-DD)

  > `TODAY()` => "2025-09-19"

- TODAY(timezone) / CURRENT_DATE(timezone) => returns date (YYYY-MM-DD) in specified timezone

  > `TODAY('Europe/Istanbul')` => "2025-09-20" (UTC+3)

- TIME() / CURRENT_TIME() => returns time (HH:MM:SS) (as a string in most databases)

  > `TIME()` => "22:37:44"

- TIME(timezone) / CURRENT_TIME(timezone) => returns time (HH:MM:SS) in specified timezone

  > `TIME('Europe/Istanbul')` => "01:37:44" (UTC+3)

- TODAY() / CURRENT_DATE() => returns date (YYYY-MM-DD)

  > `TODAY()` => "2025-09-19"

- DATEADD: unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR

  > `DATEADD(HOUR,'2025-09-18T12:34:56',2)` => 2025-09-18T14:34:56<br> > `DATEADD(SECOND,'2025-01-12T00:00:00',5)` => 2025-09-18T12:35:01

- DATEDIFF: unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR

  > `DATEDIFF(DAY,'2025-09-03','2025-09-18')` => 15<br> > `DATEDIFF(YEAR,'2025-01-03','2026-01-02')` => 0

- DAY / MONTH / YEAR

  > `YEAR('2025-09-18')` => 2025<br> > `MONTH('2025-09-18')` => 12<br> > `DAY('2025-09-18')` => 18

- DATENAME: unit can be MONTH or DAY
  > `DATENAME(MONTH, '2025-09-18')` => 'SEPTEMBER' or 'September'<br> > `DATENAME(DAY, '2025-09-18')` => 'THURSDAY' or 'Thursday'

### 6. DSL → SQL Examples

#### 1. Simple Query

My Query:

```sql
FETCH(CONCAT(LOWER(person.Name), '-', YEAR(person.CreatedAt)) AS slug, COUNT(*) AS total)
FROM Person
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

#### 2. Filter with AND/OR

My Query:

```sql
FETCH(id, first_name, last_name, gpa)
FROM students
FILTER(is_active = TRUE AND gpa >= 3.5)
ORDERBY(gpa DESC)
LIMIT(15)
```

PostgreSQL:

```sql
SELECT id, first_name, last_name, gpa
FROM students
WHERE is_active = TRUE AND gpa >= 3.5
ORDER BY gpa DESC
LIMIT 15
```

#### 3. JOIN with Having Filter

My Query:

```sql
FETCH(courses.name, COUNT(enrollments.id) AS enroll_cnt)
FROM courses
INCLUDE(enrollments)
GROUPBY(courses.name)
HAVING(enroll_cnt > 5)
ORDERBY(enroll_cnt DESC)
```

PostgreSQL:

```sql
SELECT c.name, COUNT(e.id) AS enroll_cnt
FROM courses c LEFT JOIN enrollments e ON e.course_id = c.id
GROUP BY c.name
HAVING COUNT(e.id) > 5
ORDER BY enroll_cnt DESC
```

#### 4. Conditional Logic

My Query:

```sql
FETCH(students.id, students.first_name, IFS(students.gpa >= 3.5, 'A', students.gpa >= 3.0, 'B', students.gpa >= 2.5, 'C', 'D') AS gpa_band)
FROM students
ORDERBY(gpa_band ASC, students.last_name ASC)
LIMIT(30)
```

PostgreSQL:

```sql
SELECT st.id, st.first_name,
  CASE WHEN st.gpa >= 3.5 THEN 'A'
       WHEN st.gpa >= 3.0 THEN 'B'
       WHEN st.gpa >= 2.5 THEN 'C'
       ELSE 'D' END AS gpa_band
FROM students st
ORDER BY gpa_band, st.last_name
LIMIT 30
```

#### 5. Date Functions

My Query:

```sql
FETCH(YEAR(enrolled_at) AS yr, MONTH(enrolled_at) AS mo, DAY(enrolled_at) AS dy, COUNT(*) AS cnt)
FROM enrollments
FILTER(enrolled_at >= DATEADD(DAY, NOW(), -30)) GROUPBY(yr, mo, dy)
ORDERBY(yr DESC, mo DESC, dy DESC)
```

PostgreSQL:

```sql
SELECT EXTRACT(YEAR FROM enrolled_at) AS yr,
       EXTRACT(MONTH FROM enrolled_at) AS mo,
       EXTRACT(DAY FROM enrolled_at) AS dy,
       COUNT(*) AS cnt
FROM enrollments
WHERE enrolled_at >= NOW() - INTERVAL '30 days'
GROUP BY EXTRACT(YEAR FROM enrolled_at), EXTRACT(MONTH FROM enrolled_at), EXTRACT(DAY FROM enrolled_at)
ORDER BY yr DESC, mo DESC, dy DESC
```

#### 6. Aggregate Functions with HAVING

My Query:

```sql
FETCH(school_id, AVG(salary) AS avg_salary, MIN(salary) AS min_salary, MAX(salary) AS max_salary)
FROM teachers GROUPBY(school_id)
HAVING(COUNT(*) > 2)
ORDERBY(avg_salary DESC)
```

PostgreSQL:

```sql
SELECT school_id, AVG(salary) AS avg_salary, MIN(salary) AS min_salary, MAX(salary) AS max_salary
FROM teachers
GROUP BY school_id
HAVING COUNT(*) > 2
ORDER BY avg_salary DESC
```
