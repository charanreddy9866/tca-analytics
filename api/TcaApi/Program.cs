// Read-only API over the TCA analytics tables built by the Airflow pipeline.
//
//   GET /health                                   data source check
//   GET /api/trades/{orderId}/tca                 trade-level TCA for one order
//   GET /api/reports/daily?date=2025-09-30        daily summary by broker and strategy
//   GET /api/reports/brokers?from=...&to=...      broker scorecard over a date range
//
// Data source (configuration key "DataSource"):
//   Csv        (default) serves reports/*.csv produced by the local pipeline run
//   Snowflake  queries Snowflake through the SQL API; needs Snowflake:Account, Snowflake:Token,
//              and optionally Database / Schema / Warehouse / Role / TokenType (user secrets or env vars)

using TcaApi;

var builder = WebApplication.CreateBuilder(args);

if (builder.Configuration["DataSource"] == "Snowflake")
{
    var opts = SnowflakeOptions.From(builder.Configuration);
    builder.Services.AddSingleton(opts);
    builder.Services.AddHttpClient<ITcaRepository, SnowflakeSqlApiRepository>();
}
else
{
    var reports = builder.Configuration["ReportsDir"]
        ?? Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, "..", "..", "reports"));
    builder.Services.AddSingleton<ITcaRepository>(new CsvTcaRepository(reports));
}

var app = builder.Build();

app.MapGet("/health", async (ITcaRepository repo) =>
    await repo.PingAsync() ? Results.Ok(new { status = "ok" }) : Results.Problem("Data source unreachable"));

app.MapGet("/api/trades/{orderId:long}/tca", async (long orderId, ITcaRepository repo) =>
{
    var trade = await repo.GetTradeAsync(orderId);
    return trade is null ? Results.NotFound() : Results.Ok(trade);
});

app.MapGet("/api/reports/daily", async (DateOnly date, ITcaRepository repo) =>
    Results.Ok(await repo.GetDailySummaryAsync(date)));

app.MapGet("/api/reports/brokers", async (DateOnly from, DateOnly to, ITcaRepository repo) =>
    to < from ? Results.BadRequest("'to' must be on or after 'from'.")
              : Results.Ok(await repo.GetBrokerScorecardAsync(from, to)));

app.Run();

public partial class Program;
