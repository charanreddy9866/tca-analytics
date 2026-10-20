namespace TcaApi;

public record TradeTca(
    long OrderId, DateOnly TradeDate, string Symbol, string Side, string Broker, string Strategy,
    decimal OrderQty, decimal FilledQty, decimal FillRate,
    decimal ArrivalPrice, decimal AvgExecPrice, decimal IntervalVwap, decimal ClosePrice,
    decimal SlippagePerShare, decimal SlippageCost, decimal ArrivalSlippageBps, decimal VwapSlippageBps,
    decimal CommissionCost, decimal OpportunityCost, decimal ImplementationShortfall, decimal ImplementationShortfallBps);

public record DailySummary(
    DateOnly TradeDate, string Broker, string Strategy, long Orders,
    decimal ExecutedNotional, decimal SlippageCost, decimal CommissionCost, decimal ImplementationShortfall,
    decimal WavgArrivalSlippageBps, decimal WavgVwapSlippageBps, decimal ImplementationShortfallBps, decimal AvgFillRate);

public record BrokerScore(
    string Broker, long Orders, decimal ExecutedNotional,
    decimal ArrivalSlippageBps, decimal VwapSlippageBps, decimal ImplementationShortfallBps, decimal ImplementationShortfall);

public interface ITcaRepository
{
    Task<bool> PingAsync();
    Task<TradeTca?> GetTradeAsync(long orderId);
    Task<IReadOnlyList<DailySummary>> GetDailySummaryAsync(DateOnly date);
    Task<IReadOnlyList<BrokerScore>> GetBrokerScorecardAsync(DateOnly from, DateOnly to);
}

/// <summary>Maps a row (column name -> string value) to the record types. Shared by both repositories.</summary>
public static class RowMapper
{
    static decimal D(IReadOnlyDictionary<string, string?> r, string c) =>
        decimal.Parse(r[c] ?? "0", System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture);
    static DateOnly Date(IReadOnlyDictionary<string, string?> r, string c) => DateOnly.Parse(r[c]![..10]);

    public static TradeTca Trade(IReadOnlyDictionary<string, string?> r) => new(
        long.Parse(r["order_id"]!), Date(r, "trade_date"), r["symbol"]!, r["side"]!, r["broker"]!, r["strategy"]!,
        D(r, "order_qty"), D(r, "filled_qty"), D(r, "fill_rate"),
        D(r, "arrival_price"), D(r, "avg_exec_price"), D(r, "interval_vwap"), D(r, "close_price"),
        D(r, "slippage_per_share"), D(r, "slippage_cost"), D(r, "arrival_slippage_bps"), D(r, "vwap_slippage_bps"),
        D(r, "commission_cost"), D(r, "opportunity_cost"), D(r, "implementation_shortfall"), D(r, "implementation_shortfall_bps"));

    public static DailySummary Daily(IReadOnlyDictionary<string, string?> r) => new(
        Date(r, "trade_date"), r["broker"]!, r["strategy"]!, (long)D(r, "orders"),
        D(r, "executed_notional"), D(r, "slippage_cost"), D(r, "commission_cost"), D(r, "implementation_shortfall"),
        D(r, "wavg_arrival_slippage_bps"), D(r, "wavg_vwap_slippage_bps"), D(r, "implementation_shortfall_bps"), D(r, "avg_fill_rate"));
}
