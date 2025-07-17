package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DataCleaner {
    
    /**
     * 符号标准化函数
     */
    public static class SymbolNormalizer implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private static final Map<String, Map<String, String>> MAPPINGS = new HashMap<String, Map<String, String>>() {{
            // Kraken 的特殊符号
            put("kraken", new HashMap<String, String>() {{
                put("XBTUSD", "BTC/USD");
                put("XBTUSDT", "BTC/USDT");
                put("ETHUSD", "ETH/USD");
            }});

            // Coinbase 用 - 分隔
            put("coinbase", new HashMap<String, String>() {{
                put("BTC-USD", "BTC/USD");
                put("ETH-USD", "ETH/USD");
                put("SOL-USD", "SOL/USD");
            }});
        }};

        public String normalizeSymbol(String symbol, String exchange) {
            if (symbol == null) return null;

            String exchangeLower = exchange.toLowerCase();
            if (MAPPINGS.containsKey(exchangeLower)) {
                Map<String, String> exchangeMap = MAPPINGS.get(exchangeLower);
                if (exchangeMap.containsKey(symbol)) {
                    return exchangeMap.get(symbol);
                }
            }

            String normalized = symbol.toUpperCase()
                    .replace("-", "/")
                    .replace("_", "/");

            if (!normalized.contains("/")) {
                String[] quotes = {"USDT", "USDC", "USD", "EUR"};
                for (String quote : quotes) {
                    if (normalized.endsWith(quote)) {
                        String base = normalized.substring(0, normalized.length() - quote.length());
                        if (base.length() >= 2) {
                            return base + "/" + quote;
                        }
                    }
                }
            }

            return normalized;
        }
    }
    
    /**
     * 价格数据清洗映射器
     */
    public static class PriceDataCleaner implements MapFunction<PriceRecord, PriceRecord>, Serializable {
        private static final long serialVersionUID = 1L;
        private transient SymbolNormalizer symbolNormalizer;
        
        @Override
        public PriceRecord map(PriceRecord record) throws Exception {
            // 检查输入记录是否为空
            if (record == null) {
                return null;
            }
            
            // 检查必要字段是否为空
            if (record.symbol == null || record.exchange == null) {
                System.err.println("跳过无效记录: symbol=" + record.symbol + ", exchange=" + record.exchange);
                return null;
            }
            
            // 延迟初始化，避免序列化问题
            if (symbolNormalizer == null) {
                symbolNormalizer = new SymbolNormalizer();
            }
            
            // 1. 标准化符号
            String normalizedSymbol = symbolNormalizer.normalizeSymbol(record.symbol, record.exchange);
            if (normalizedSymbol == null) {
                System.err.println("符号标准化失败: " + record.symbol + " from " + record.exchange);
                return null;
            }
            
            // 2. 验证价格数据
            if (record.last <= 0 || record.bid <= 0 || record.ask <= 0) {
                System.err.println("跳过无效价格数据: last=" + record.last + ", bid=" + record.bid + ", ask=" + record.ask);
                return null; // 过滤无效价格
            }
            
            // 3. 验证买卖价差
            double spread = record.ask - record.bid;
            double spreadRatio = spread / record.last;
            if (spreadRatio > 0.1) { // 价差超过10%认为是异常
                System.err.println("跳过异常价差: spread=" + spread + ", ratio=" + spreadRatio);
                return null;
            }
            
            // 4. 创建清洗后的记录
            PriceRecord cleaned = new PriceRecord();
            cleaned.exchange = record.exchange.toLowerCase(); // 标准化交易所名称
            cleaned.symbol = normalizedSymbol;
            cleaned.last = Math.round(record.last * 100.0) / 100.0; // 保留2位小数
            cleaned.bid = Math.round(record.bid * 100.0) / 100.0;
            cleaned.ask = Math.round(record.ask * 100.0) / 100.0;
            cleaned.high = Math.round(record.high * 100.0) / 100.0;
            cleaned.low = Math.round(record.low * 100.0) / 100.0;
            cleaned.volume = Math.round(record.volume * 10000.0) / 10000.0; // 保留4位小数
            cleaned.timestamp = record.timestamp; // 保持时间戳不变
            
            return cleaned;
        }
    }
} 