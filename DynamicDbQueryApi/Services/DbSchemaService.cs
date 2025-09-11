using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Services
{
    public class DbSchemaService : IDbSchemaService
    {
        private readonly ILogger<DbSchemaService> _logger;
        public DbSchemaService(ILogger<DbSchemaService> logger)
        {
            _logger = logger;
        }

        // Belirtilen tablolara ait foreign key çiftini döner
        public async Task<ForeignKeyPair?> GetForeignKeyPairAsync(IDbConnection connection, string dbType, string fromTable, string includeTable)
        {
            var includeSql = GetIncludeQuery(dbType, fromTable, includeTable);

            var includeResult = await connection.QueryFirstOrDefaultAsync(includeSql);
            _logger.LogInformation("Include Query Result: {Result}", JsonSerializer.Serialize(includeResult as object));

            if (includeResult != null)
            {
                var pair = new ForeignKeyPair();
                IDictionary<string, object>? dict = includeResult as IDictionary<string, object>;

                if (dict != null)
                {
                    if (dbType.ToLower() == "oracle")
                    {
                        // Oracle kolon isimleri büyük harfli döner
                        pair.ForeignKey = dict["FK_COLUMN"]?.ToString() ?? "";
                        pair.ReferencedKey = dict["REFERENCED_COLUMN"]?.ToString() ?? "";
                    }
                    else
                    {
                        pair.ForeignKey = dict["fk_column"]?.ToString() ?? "";
                        pair.ReferencedKey = dict["referenced_column"]?.ToString() ?? "";
                    }
                }
                return pair;
            }
            return null;
        }

        // DB türüne göre foreign key sorgusu oluşturma
        public string GetIncludeQuery(string dbType, string fromTable, string includeTable)
        {
            var type = dbType.ToLower();
            if (type == "postgresql" || type == "postgres")
            {
                return $@"
                    SELECT kcu.column_name AS fk_column, ccu.column_name AS referenced_column
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{fromTable}'
                    AND ccu.table_name = '{includeTable}'";
            }
            else if (type == "mssql" || type == "sqlserver")
            {
                return $@"
                    SELECT cp.name AS fk_column, cr.name AS referenced_column
                    FROM sys.foreign_keys AS fk
                    INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
                    INNER JOIN sys.tables AS tp ON fkc.parent_object_id = tp.object_id
                    INNER JOIN sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                    INNER JOIN sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
                    INNER JOIN sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                    WHERE tp.name = '{fromTable}' AND tr.name = '{includeTable}'";
            }
            else if (type == "mysql")
            {
                return $@"
                    SELECT k.COLUMN_NAME AS fk_column, k.REFERENCED_COLUMN_NAME AS referenced_column
                    FROM information_schema.KEY_COLUMN_USAGE k
                    WHERE k.TABLE_NAME = '{fromTable}'
                    AND k.REFERENCED_TABLE_NAME = '{includeTable}'
                    AND k.REFERENCED_COLUMN_NAME IS NOT NULL";
            }
            else if (type == "oracle")
            {
                return $@"
                    SELECT a.column_name AS fk_column, c_pk.column_name AS referenced_column
                    FROM user_cons_columns a
                    JOIN user_constraints c ON a.constraint_name = c.constraint_name
                    JOIN user_cons_columns c_pk ON c.r_constraint_name = c_pk.constraint_name
                    WHERE c.constraint_type = 'R'
                    AND a.table_name = '{fromTable.ToUpper()}'
                    AND c_pk.table_name = '{includeTable.ToUpper()}'";
            }
            else
            {
                throw new NotSupportedException($"Database type is not supported for INCLUDE queries.");
            }

        }

        // DB türüne göre tablo isimlerini alma sorgusu oluşturma
        public string GetTablesQuery(string dbType)
        {
            var type = dbType.ToLower();

            if (type == "postgresql" || type == "postgres")
            {
                return @"
                    SELECT table_name AS TableName
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'";
            }
            else if (type == "mssql" || type == "sqlserver")
            {
                return @"
                    SELECT TABLE_NAME AS TableName
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'";
            }
            else if (type == "mysql")
            {
                return @"
                    SELECT TABLE_NAME AS TableName
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = DATABASE()";
            }
            else if (type == "oracle")
            {
                return @"
                    SELECT table_name AS TableName
                    FROM user_tables";
            }
            else
            {
                throw new NotSupportedException($"Database type {dbType} is not supported for inspection.");
            }
        }

        // DB türüne göre tablo kolonlarını alma sorgusu oluşturma
        public string GetColumnsQuery(string dbType, string tableName)
        {
            var type = dbType.ToLower();

            if (type == "postgresql" || type == "postgres")
            {
                return $@"
                        SELECT column_name   AS Name,
                               data_type     AS DataType,
                               is_nullable   AS IsNullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = '{tableName}'";
            }
            else if (type == "mssql" || type == "sqlserver")
            {
                return $@"
                        SELECT COLUMN_NAME   AS Name,
                               DATA_TYPE     AS DataType,
                               IS_NULLABLE   AS IsNullable
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{tableName}'";
            }
            else if (type == "mysql")
            {
                return $@"
                        SELECT COLUMN_NAME   AS Name,
                               DATA_TYPE     AS DataType,
                               IS_NULLABLE   AS IsNullable
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{tableName}'";
            }
            else if (type == "oracle")
            {
                return $@"
                        SELECT column_name   AS Name,
                               data_type     AS DataType,
                               nullable      AS IsNullable
                        FROM user_tab_columns
                        WHERE table_name = '{tableName.ToUpper()}'";
            }
            else
            {
                throw new NotSupportedException($"Database type {dbType} is not supported for inspection.");
            }
        }

        public string GetRelationshipsQuery(string dbType)
        {
            var type = dbType.ToLower();

            if (type == "postgresql" || type == "postgres")
            {
                return @"
                    SELECT 
                        tc.constraint_name AS constraint_name,
                        kcu.table_name     AS fk_table,
                        kcu.column_name    AS fk_column,
                        ccu.table_name     AS pk_table,
                        ccu.column_name    AS pk_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                     AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema = 'public'";
            }
            else if (type == "mssql" || type == "sqlserver")
            {
                return @"
                    SELECT 
                        fk.name              AS constraint_name,
                        tf.name              AS fk_table,
                        cf.name              AS fk_column,
                        tp.name              AS pk_table,
                        cp.name              AS pk_column
                    FROM sys.foreign_keys fk
                    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                    INNER JOIN sys.tables tf ON fkc.parent_object_id = tf.object_id
                    INNER JOIN sys.columns cf ON fkc.parent_object_id = cf.object_id AND fkc.parent_column_id = cf.column_id
                    INNER JOIN sys.tables tp ON fkc.referenced_object_id = tp.object_id
                    INNER JOIN sys.columns cp ON fkc.referenced_object_id = cp.object_id AND fkc.referenced_column_id = cp.column_id";
            }
            else if (type == "mysql")
            {
                return @"
                    SELECT 
                        rc.CONSTRAINT_NAME      AS constraint_name,
                        kcu.TABLE_NAME          AS fk_table,
                        kcu.COLUMN_NAME         AS fk_column,
                        kcu.REFERENCED_TABLE_NAME AS pk_table,
                        kcu.REFERENCED_COLUMN_NAME AS pk_column
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                      ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                     AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                    WHERE kcu.CONSTRAINT_SCHEMA = DATABASE()
                      AND kcu.REFERENCED_TABLE_NAME IS NOT NULL";
            }
            else if (type == "oracle")
            {
                return @"
                    SELECT 
                        ac.constraint_name        AS constraint_name,
                        aco.table_name            AS fk_table,
                        aco.column_name           AS fk_column,
                        ac_r.table_name           AS pk_table,
                        acc.column_name           AS pk_column
                    FROM user_constraints ac
                    JOIN user_cons_columns aco ON ac.constraint_name = aco.constraint_name
                    JOIN user_constraints ac_r ON ac.r_constraint_name = ac_r.constraint_name
                    JOIN user_cons_columns acc ON ac_r.constraint_name = acc.constraint_name AND acc.position = aco.position
                    WHERE ac.constraint_type = 'R'";
            }
            else
            {
                throw new NotSupportedException($"Database type {dbType} is not supported for inspection.");
            }
        }
    }
}