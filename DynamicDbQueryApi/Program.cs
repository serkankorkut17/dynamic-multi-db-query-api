using DynamicDbQueryApi.Interfaces;
using DynamicDbQueryApi.Repositories;
using DynamicDbQueryApi.Services;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddScoped<IQueryService, QueryService>();
builder.Services.AddScoped<IQueryParserService, QueryParserService>();
builder.Services.AddScoped<ISqlBuilderService, SqlBuilderService>();
// builder.Services.AddScoped<IDbSchemaService, DbSchemaService>();
builder.Services.AddScoped<ISqlProvider, SqlProvider>();
builder.Services.AddScoped<IQueryRepository, QueryRepository>();

// Serilog
builder.Host.UseSerilog((context, loggerConfig) =>
{
    loggerConfig.ReadFrom.Configuration(context.Configuration);
});

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

if (!app.Environment.IsDevelopment())
{
    app.UseHttpsRedirection();
}

app.UseSerilogRequestLogging();

app.UseAuthorization();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapFallbackToFile("index.html");

app.MapControllers();

app.Run();
