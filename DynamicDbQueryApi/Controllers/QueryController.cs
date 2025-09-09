using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace DynamicDbQueryApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class QueryController : ControllerBase
    {
        private readonly IQueryService _queryService;

        public QueryController(IQueryService queryService)
        {
            _queryService = queryService;
        }

        [HttpPost]
        public async Task<IActionResult> PostQuery([FromBody] QueryRequestDTO request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                var result = await _queryService.MyQueryAsync(request);
                var test = result.ToList();
                // return type json
                return Ok(test);
            }
            catch (NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }
            // catch (Exception ex)
            // {
            //     return StatusCode(500, $"Internal server error: {ex.Message}");
            // }
        }

        [HttpPost("sql")]
        public async Task<IActionResult> SQLQuery([FromBody] QueryRequestDTO request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                var result = await _queryService.SQLQueryAsync(request);
                return Ok(result);
            }
            catch (NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [HttpPost("inspect")]
        public async Task<IActionResult> InspectDatabase([FromBody] QueryRequestDTO request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                var result = await _queryService.InspectDatabaseAsync(request);
                return Ok(result);
            }
            catch (NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}