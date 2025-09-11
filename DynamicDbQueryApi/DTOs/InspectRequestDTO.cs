using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public class InspectRequestDTO
    {
        [Required]
        public string DbType { get; set; } = "";
        [Required]
        public string ConnectionString { get; set; } = "";

    }
}