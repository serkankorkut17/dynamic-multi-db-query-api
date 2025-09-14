using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class RelationshipModel
    {
        public string ConstraintName { get; set; } = "";
        public string FkTable { get; set; } = "";
        public string FkColumn { get; set; } = "";
        public string PkTable { get; set; } = "";
        public string PkColumn { get; set; } = "";
    }
}