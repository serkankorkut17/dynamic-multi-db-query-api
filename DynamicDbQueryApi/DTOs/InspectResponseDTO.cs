using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.Entities;

namespace DynamicDbQueryApi.DTOs
{
    public class InspectResponseDTO
    {
        public List<TableModel> Tables { get; set; } = new();
        public List<RelationshipModel> Relationships { get; set; } = new();
    }
}