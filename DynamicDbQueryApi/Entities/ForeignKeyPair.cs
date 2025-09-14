using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class ForeignKeyPair
    {
        public string ForeignKey { get; set; } = "";
        public string ReferencedKey { get; set; } = "";
    }
}