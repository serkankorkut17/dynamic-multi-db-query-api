using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class ColumnModel
    {
        public string Name { get; set; } = "";
        public string DataType { get; set; } = "";
        public bool IsNullable { get; set; }
    }
}