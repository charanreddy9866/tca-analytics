namespace TcaApi;

/// <summary>
/// Local-mode repository that serves the CSVs the pipeline writes to reports/.
/// Lets the API be run and tested without a Snowflake account.
/// </summary>
public class CsvTcaRepository : ITcaRepository
{
    private readonly List<TradeTca> _trades;
    private readonly List<DailySummary> _daily;

    public CsvTcaRepository(string reportsDir)
    {
        _trades = Read(Path.Combine(reportsDir, "trade_tca.csv")).Select(RowMapper.Trade).ToList();
        _daily = Read(Path.Combine(reportsDir, "daily_tca_summary.csv")).Select(RowMapper.Daily).ToList();
    }

    public Task<bool> PingAsync() => Task.FromResult(_trades.Count > 0);

    public Task<TradeTca?> GetTradeAsync(long orderId) =>
        Task.FromResult(_trades.FirstOrDefault(t => t.OrderId == orderId));

    public Task<IReadOnlyList<DailySummary>> GetDailySummaryAsync(DateOnly date) =>
        Task.FromResult<IReadOnlyList<DailySummary>>(
            _daily.Where(d => d.TradeDate == date).OrderBy(d => d.Broker).ThenBy(d => d.Strategy).ToList());

    public Task<IReadOnlyList<BrokerScore>> GetBrokerScorecardAsync(DateOnly from, DateOnly to)
    {
        var scores = _trades
            .Where(t => t.TradeDate >= from && t.TradeDate <= to)
            .GroupBy(t => t.Broker)
            .Select(g =>
            {
                var notional = g.Sum(t => t.FilledQty * t.AvgExecPrice);
                var benchmark = g.Sum(t => t.OrderQty * t.ArrivalPrice);
                var shortfall = g.Sum(t => t.ImplementationShortfall);
                return new BrokerScore(
                    g.Key, g.Count(), Math.Round(notional, 2),
                    Math.Round(g.Sum(t => t.ArrivalSlippageBps * t.FilledQty * t.AvgExecPrice) / notional, 2),
                    Math.Round(g.Sum(t => t.VwapSlippageBps * t.FilledQty * t.AvgExecPrice) / notional, 2),
                    Math.Round(shortfall / benchmark * 10000m, 2),
                    Math.Round(shortfall, 2));
            })
            .OrderBy(s => s.ImplementationShortfallBps)
            .ToList();
        return Task.FromResult<IReadOnlyList<BrokerScore>>(scores);
    }

    private static IEnumerable<Dictionary<string, string?>> Read(string path)
    {
        using var reader = new StreamReader(path);
        var header = reader.ReadLine()!.Split(',');
        while (reader.ReadLine() is { } line)
        {
            var values = line.Split(',');   // pipeline output contains no quoted commas
            yield return header.Select((h, i) => (h, (string?)values[i])).ToDictionary(x => x.h, x => x.Item2);
        }
    }
}
