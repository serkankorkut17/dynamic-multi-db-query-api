using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public class QueryRequestDTO
    {
        [Required]
        public string DbType { get; set; } = "";
        [Required]
        public string ConnectionString { get; set; } = "";
        [Required]
        public string Query { get; set; } = "";
        // Output db
        public bool WriteToOutputDb { get; set; } = false;
        public string? OutputDbType { get; set; }
        public string? OutputConnectionString { get; set; }
        public string? OutputTableName { get; set; }

    }
}