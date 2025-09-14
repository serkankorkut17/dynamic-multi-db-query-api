using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public class QueryResultDTO
    {
        public string Sql { get; set; } = "";
        public IEnumerable<dynamic> Data { get; set; } = Array.Empty<dynamic>();
        public bool WrittenToOutputDb { get; set; } = false;
    }
}