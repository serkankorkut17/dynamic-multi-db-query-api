using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public class IncludeModel
    {
        public string Table { get; set; } = "";
        public string ForeignKey { get; set; } = "";
        public string ReferencedKey { get; set; } = "";
    }
}