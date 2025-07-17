package org.example;

import java.util.ArrayList;
import java.util.List;

public class PriceStats {
    public String symbol;                    // 货币符号
    public double highestPrice;              // 最高价格
    public String highestExchange;           // 最高价格交易所
    public double lowestPrice;               // 最低价格
    public String lowestExchange;            // 最低价格交易所
    public int recordCount;                  // 记录数量
    public long windowStart;                 // 时间窗口开始时间
    public long windowEnd;                   // 时间窗口结束时间
    public long earliestTimestamp;           // 最早的时间戳
    public long latestTimestamp;             // 最晚的时间戳
    public List<PriceRecord> rawRecords;     // 原始记录列表

    public PriceStats() {
        this.highestPrice = Double.NEGATIVE_INFINITY;  // 修复：使用负无穷大作为最高价初始值
        this.lowestPrice = Double.POSITIVE_INFINITY;   // 修复：使用正无穷大作为最低价初始值
        this.recordCount = 0;
        this.earliestTimestamp = Long.MAX_VALUE;
        this.latestTimestamp = Long.MIN_VALUE;
        this.rawRecords = new ArrayList<>();
    }

    public PriceStats(String symbol) {
        this.symbol = symbol;
        this.highestPrice = Double.NEGATIVE_INFINITY;  // 修复：使用负无穷大作为最高价初始值
        this.lowestPrice = Double.POSITIVE_INFINITY;   // 修复：使用正无穷大作为最低价初始值
        this.recordCount = 0;
        this.earliestTimestamp = Long.MAX_VALUE;
        this.latestTimestamp = Long.MIN_VALUE;
        this.rawRecords = new ArrayList<>();
    }

    public void updateWithRecord(PriceRecord record) {
        // 使用last价格作为比较基准
        double price = record.last;
        
        // 更新最高价格
        if (price > highestPrice) {
            highestPrice = price;
            highestExchange = record.exchange;
        }

        // 更新最低价格
        if (price < lowestPrice) {
            lowestPrice = price;
            lowestExchange = record.exchange;
        }

        // 更新时间戳范围
        if (record.timestamp < earliestTimestamp) {
            earliestTimestamp = record.timestamp;
        }
        if (record.timestamp > latestTimestamp) {
            latestTimestamp = record.timestamp;
        }

        // 添加原始记录
        rawRecords.add(record);

        recordCount++;
    }

    public void setWindowTime(long start, long end) {
        this.windowStart = start;
        this.windowEnd = end;
    }

    @Override
    public String toString() {
        // 检查是否有有效数据
        if (highestPrice == Double.NEGATIVE_INFINITY || lowestPrice == Double.POSITIVE_INFINITY) {
            return String.format(
                "货币: %s | 无有效价格数据 | 记录数: %d | 时间范围: %d - %d",
                symbol, recordCount, earliestTimestamp, latestTimestamp
            );
        }
        
        return String.format(
            "货币: %s | 最高价: %.2f (%s) | 最低价: %.2f (%s) | 记录数: %d | 时间范围: %d - %d",
            symbol, highestPrice, highestExchange, lowestPrice, lowestExchange, recordCount,
            earliestTimestamp, latestTimestamp
        );
    }
} 