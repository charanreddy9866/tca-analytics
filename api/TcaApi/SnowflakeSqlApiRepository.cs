using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace TcaApi;

/// <summary>
/// Queries Snowflake through the Snowflake SQL API (POST /api/v2/statements).
/// Uses only HttpClient and System.Text.Json, so the service has no third-party dependencies.
/// All queries use positional bind variables (?) - no string concatenation of user input.
/// </summary>
public class SnowflakeSqlApiRepository(HttpClient http, SnowflakeOptions opts) : ITcaRepository
{
    public async Task<bool> PingAsync()
    {
        try { return (await QueryAsync("SELECT 1 AS ok")).Count == 1; }
        catch (HttpRequestException) { return false; }
    }

    public async Task<TradeTca?> GetTradeAsync(long orderId)
    {
        var rows = await QueryAsync("SELECT * FROM analytics.trade_tca WHERE order_id = ?", ("FIXED", orderId.ToString()));
        return rows.Count == 0 ? null : RowMapper.Trade(rows[0]);
    }

    public async Task<IReadOnlyList<DailySummary>> GetDailySummaryAsync(DateOnly date)
    {
        var rows = await QueryAsync(
            "SELECT * FROM analytics.daily_tca_summary WHERE trade_date = TO_DATE(?) ORDER BY broker, strategy",
            ("TEXT", date.ToString("yyyy-MM-dd")));
        return rows.Select(RowMapper.Daily).ToList();
    }

    public async Task<IReadOnlyList<BrokerScore>> GetBrokerScorecardAsync(DateOnly from, DateOnly to)
    {
        var rows = await QueryAsync(
            """
            SELECT broker,
                   COUNT(*) AS orders,
                   SUM(executed_notional) AS executed_notional,
                   SUM(arrival_slippage_bps * executed_notional) / SUM(executed_notional) AS arrival_slippage_bps,
                   SUM(vwap_slippage_bps * executed_notional) / SUM(executed_notional)    AS vwap_slippage_bps,
                   SUM(implementation_shortfall) / SUM(order_qty * arrival_price) * 10000 AS implementation_shortfall_bps,
                   SUM(implementation_shortfall) AS implementation_shortfall
            FROM analytics.trade_tca
            WHERE trade_date BETWEEN TO_DATE(?) AND TO_DATE(?)
            GROUP BY broker
            ORDER BY implementation_shortfall_bps
            """,
            ("TEXT", from.ToString("yyyy-MM-dd")), ("TEXT", to.ToString("yyyy-MM-dd")));
        return rows.Select(r => new BrokerScore(
            r["broker"]!, long.Parse(r["orders"]!), decimal.Parse(r["executed_notional"]!),
            decimal.Parse(r["arrival_slippage_bps"]!), decimal.Parse(r["vwap_slippage_bps"]!),
            decimal.Parse(r["implementation_shortfall_bps"]!), decimal.Parse(r["implementation_shortfall"]!))).ToList();
    }

    private async Task<List<Dictionary<string, string?>>> QueryAsync(string sql, params (string Type, string Value)[] binds)
    {
        var body = new Dictionary<string, object?>
        {
            ["statement"] = sql,
            ["timeout"] = 60,
            ["database"] = opts.Database,
            ["schema"] = opts.Schema,
            ["warehouse"] = opts.Warehouse,
            ["role"] = opts.Role,
            ["bindings"] = binds.Select((b, i) => (Key: (i + 1).ToString(), b))
                                .ToDictionary(x => x.Key, x => new { type = x.b.Type, value = x.b.Value }),
        };
        using var req = new HttpRequestMessage(HttpMethod.Post, $"https://{opts.Account}.snowflakecomputing.com/api/v2/statements")
        {
            Content = JsonContent.Create(body),
        };
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", opts.Token);
        req.Headers.Add("X-Snowflake-Authorization-Token-Type", opts.TokenType);
        req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        using var resp = await http.SendAsync(req);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync());

        var cols = doc.RootElement.GetProperty("resultSetMetaData").GetProperty("rowType")
                      .EnumerateArray().Select(c => c.GetProperty("name").GetString()!.ToLowerInvariant()).ToArray();
        return doc.RootElement.GetProperty("data").EnumerateArray()
                  .Select(row => row.EnumerateArray().Select((v, i) => (cols[i], v.ValueKind == JsonValueKind.Null ? null : v.GetString()))
                                    .ToDictionary(x => x.Item1, x => x.Item2))
                  .ToList();
    }
}

public record SnowflakeOptions(string Account, string Token, string TokenType, string Database, string Schema, string Warehouse, string? Role)
{
    public static SnowflakeOptions From(IConfiguration c) => new(
        c["Snowflake:Account"] ?? throw new InvalidOperationException("Missing Snowflake:Account"),
        c["Snowflake:Token"] ?? throw new InvalidOperationException("Missing Snowflake:Token"),
        c["Snowflake:TokenType"] ?? "KEYPAIR_JWT",
        c["Snowflake:Database"] ?? "TCA",
        c["Snowflake:Schema"] ?? "ANALYTICS",
        c["Snowflake:Warehouse"] ?? "TCA_WH",
        c["Snowflake:Role"]);
}
