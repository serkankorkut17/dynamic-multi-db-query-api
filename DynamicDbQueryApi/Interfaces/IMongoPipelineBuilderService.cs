using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.Entities.Query;
using MongoDB.Bson;

namespace DynamicDbQueryApi.Interfaces
{
    public interface IMongoPipelineBuilderService
    {
        List<BsonDocument> BuildPipeline(QueryModel model);
    }
}