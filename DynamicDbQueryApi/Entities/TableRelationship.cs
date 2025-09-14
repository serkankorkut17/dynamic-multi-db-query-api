using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class TableRelationship
    {
        public string Key { get; set; } = "";
        public string RelationTable { get; set; } = "";
        public string RelationKey { get; set; } = "";
    }
}