using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace DynamicDbQueryApi.Data
{
    public class MongoContext
    {
        private readonly string _connectionString;
        private readonly string? _databaseName;
        private IMongoClient? _client;

        public MongoContext(string connectionString, string? databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
        }

        public IMongoClient GetClient()
        {
            if (_client == null)
            {
                _client = new MongoClient(_connectionString);
            }
            return _client;
        }

        public IMongoDatabase GetDatabase()
        {
            var url = MongoUrl.Create(_connectionString);
            var dbName = _databaseName ?? url.DatabaseName;
            if (string.IsNullOrWhiteSpace(dbName))
                throw new ArgumentException("Mongo connection string must include a database or pass databaseName.");

            return GetClient().GetDatabase(dbName);
        }

        public IMongoCollection<T> GetCollection<T>(string name)
        {
            return GetDatabase().GetCollection<T>(name);
        }
    }
}