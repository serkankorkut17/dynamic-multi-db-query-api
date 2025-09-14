using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class TableModel
    {
        public string Table { get; set; } = "";
        public List<ColumnModel> Columns { get; set; } = new();
        public List<TableRelationship> Relationships { get; set; } = new();
    }
}