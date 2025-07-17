package org.example;

public class PriceRecord {
    public String exchange;      // 交易所名称
    public String symbol;        // 交易对符号
    public double last;          // 最新价格
    public double bid;           // 买价
    public double ask;           // 卖价
    public double high;          // 24小时最高价
    public double low;           // 24小时最低价
    public double volume;        // 24小时交易量
    public long timestamp;       // 数据产生的时间戳（毫秒）

    public PriceRecord() {}  // 必须要有无参构造

    public PriceRecord(String exchange, String symbol, double last, double bid, double ask, double high, double low, double volume) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.last = last;
        this.bid = bid;
        this.ask = ask;
        this.high = high;
        this.low = low;
        this.volume = volume;
        this.timestamp = System.currentTimeMillis(); // 使用当前时间作为默认时间戳
    }

    public PriceRecord(String exchange, String symbol, double last, double bid, double ask, double high, double low, double volume, long timestamp) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.last = last;
        this.bid = bid;
        this.ask = ask;
        this.high = high;
        this.low = low;
        this.volume = volume;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("PriceRecord{exchange='%s', symbol='%s', last=%.2f, bid=%.2f, ask=%.2f, high=%.2f, low=%.2f, volume=%.4f, timestamp=%d}", 
                exchange, symbol, last, bid, ask, high, low, volume, timestamp);
    }
}
