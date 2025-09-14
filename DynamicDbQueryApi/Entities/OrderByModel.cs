using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class OrderByModel
    {
        public string Column { get; set; } = "";
        public bool Desc { get; set; } = false;
    }
}