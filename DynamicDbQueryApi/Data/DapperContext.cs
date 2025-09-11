using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace DynamicDbQueryApi.Data
{
    public class DapperContext
    {
        private readonly string _dbType;
        private readonly string _connectionString;

        public DapperContext(string dbType, string connectionString)
        {
            _dbType = dbType;
            _connectionString = connectionString;
        }

        public IDbConnection CreateConnection()
        {
            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                throw new ArgumentException("Connection string is null or empty.");
            }

            var type = _dbType.ToLower();

            if (type == "postgresql" || type == "postgres")
            {
                return new NpgsqlConnection(_connectionString);
            }
            if (type == "mssql" || type == "sqlserver")
            {
                return new SqlConnection(_connectionString);
            }
            if (type == "mysql")
            {
                return new MySqlConnection(_connectionString);
            }
            if (type == "oracle")
            {
                return new OracleConnection(_connectionString);
            }
            else
            {
                throw new NotSupportedException($"Database type is not supported.");
            }
        }

        public async Task<IDbConnection> GetOpenConnectionAsync()
        {
            var connection = CreateConnection();
            try
            {
                await ((DbConnection)connection).OpenAsync();
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to open database connection.", ex);
            }
            return connection;
        }
    }
}