package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;  // 这个很重要！

import java.time.Duration;
import java.util.Properties;
// 在你的CryptoCyclePriceProcessor类中添加这个方法
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 测试数据库连接和表结构
 */



/**
 * 专门针对加密货币周期数据获取模式的处理器
 * 
 * 数据获取模式：
 * - 每个币种10秒一个周期
 * - 不同币种间隔2秒
 * - 例如：第0秒BTC，第2秒ETH，第4秒ADA，第6秒DOT，第8秒LINK，第10秒又回到BTC
 */
public class CryptoCyclePriceProcessor {
    public static void main(String[] args) throws Exception {
        // 打印配置信息
        ConfigLoader.printConfig();
        
        // 从配置文件加载参数
        String kafkaTopic = ConfigLoader.getString("kafka.topic", "price_topic");
        String kafkaBootstrapServers = ConfigLoader.getString("kafka.bootstrap.servers", "localhost:9092");
        String kafkaGroupId = ConfigLoader.getString("kafka.group.id", "flink-crypto-cycle-processor");
        
        String timescaleDbUrl = ConfigLoader.getString("timescaledb.url", "jdbc:postgresql://localhost:15432/crypto_data");
        String timescaleDbUsername = ConfigLoader.getString("timescaledb.username", "postgres");
        String timescaleDbPassword = ConfigLoader.getString("timescaledb.password", "password");
        
        int cyclePeriodSeconds = ConfigLoader.getInt("window.cycle.period.seconds", 10);
        int currencyIntervalSeconds = ConfigLoader.getInt("window.currency.interval.seconds", 2);
        int maxDelaySeconds = ConfigLoader.getInt("window.max.delay.seconds", 3);
        int timeDiffThresholdMs = ConfigLoader.getInt("window.time.diff.threshold.ms", 5000);
        int idleTimeoutSeconds = ConfigLoader.getInt("window.idle.timeout.seconds", 15);
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrapServers);
        props.setProperty("group.id", kafkaGroupId);
        props.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                kafkaTopic,
                new SimpleStringSchema(),
                props
        );
        consumer.setStartFromEarliest();

        // 3. 创建配置好的ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 4. 数据流处理管道
        // 4.1 创建清洗后的原始数据流
        DataStream<PriceRecord> cleanedDataStream = env
                .addSource(consumer)
                // 第一步：JSON反序列化
                .map(json -> {
                    try {
                        PriceRecord record = objectMapper.readValue(json, PriceRecord.class);
                        // 如果没有时间戳，使用当前时间
                        if (record.timestamp == 0) {
                            record.timestamp = System.currentTimeMillis();
                        }
                        return record;
                    } catch (Exception e) {
                        System.err.println("JSON解析失败: " + json + ", 错误: " + e.getMessage());
                        return null;
                    }
                })
                .returns(PriceRecord.class)
                
                // 第二步：数据清洗
                .map(new DataCleaner.PriceDataCleaner())
                
                // 第三步：过滤掉null值（清洗失败的数据）
                .filter(record -> record != null);

        // 4.2 创建聚合后的统计数据流
        DataStream<PriceStats> aggregatedDataStream = cleanedDataStream
                // 第四步：分配水位线（针对币种周期优化）
                .assignTimestampsAndWatermarks(
                    PriceWatermarkStrategy.createCryptoCycleWatermarkStrategy()
                )
                
                // 第五步：按货币分组
                .keyBy(r -> r.symbol)
                
                // 第六步：使用事件时间窗口（10秒窗口，对应币种周期）
                .window(TumblingEventTimeWindows.of(Time.seconds(cyclePeriodSeconds)))
                
                // 第七步：计算价格统计
                .aggregate(new CryptoCyclePriceStatsAggregate(timeDiffThresholdMs));

        // 5. 写入TimescaleDB - 两个数据流分别写入不同表
        // 5.1 写入原始清洗数据
        System.out.println("=== 数据库连接信息 ===");
        System.out.println("URL: " + timescaleDbUrl);
        System.out.println("Username: " + timescaleDbUsername);
        System.out.println("Password: " + (timescaleDbPassword != null ? "***" : "null"));
        System.out.println("======================");
        
        cleanedDataStream
            .map(record -> {
                System.out.println("准备写入原始数据: " + record.symbol + " @ " + record.exchange + " = " + record.last);
                return record;
            })
            .addSink(
            JdbcSink.sink(
                "INSERT INTO crypto_raw_prices (exchange, symbol, last, bid, ask, high, low, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<PriceRecord>) (ps, record) -> {
                    try {
                        ps.setString(1, record.exchange);
                        ps.setString(2, record.symbol);
                        ps.setDouble(3, record.last);
                        ps.setDouble(4, record.bid);
                        ps.setDouble(5, record.ask);
                        ps.setDouble(6, record.high);
                        ps.setDouble(7, record.low);
                        ps.setDouble(8, record.volume);
                        ps.setLong(9, record.timestamp);
                        System.out.println("SQL参数设置完成: " + record.symbol);
                    } catch (Exception e) {
                        System.err.println("SQL参数设置失败: " + e.getMessage());
                        throw e;
                    }
                },
                JdbcExecutionOptions.builder()
                .withBatchSize(1)
                .withBatchIntervalMs(0)
                .withMaxRetries(3)
                .build(),  // 这个build()很关键
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(timescaleDbUrl)
                    .withDriverName("org.postgresql.Driver")
                    .withUsername(timescaleDbUsername)
                    .withPassword(timescaleDbPassword)
                    .build()
            )
        ).name("Raw Data TimescaleDB Sink").setParallelism(1);

        // 5.2 写入聚合统计数据
        aggregatedDataStream
            .map(stats -> {
                System.out.println("准备写入聚合数据: " + stats.symbol + " 最高=" + stats.highestPrice + " 最低=" + stats.lowestPrice);
                return stats;
            })
            .addSink(
            JdbcSink.sink(
                "INSERT INTO crypto_price_stats (symbol, highest_price, highest_exchange, lowest_price, lowest_exchange, record_count, earliest_timestamp, latest_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<PriceStats>) (ps, stats) -> {
                    try {
                        ps.setString(1, stats.symbol);
                        ps.setDouble(2, stats.highestPrice);
                        ps.setString(3, stats.highestExchange);
                        ps.setDouble(4, stats.lowestPrice);
                        ps.setString(5, stats.lowestExchange);
                        ps.setInt(6, stats.recordCount);
                        ps.setLong(7, stats.earliestTimestamp);
                        ps.setLong(8, stats.latestTimestamp);
                        System.out.println("聚合SQL参数设置完成: " + stats.symbol);
                    } catch (Exception e) {
                        System.err.println("聚合SQL参数设置失败: " + e.getMessage());
                        throw e;
                    }
                },
                JdbcExecutionOptions.builder()
                .withBatchSize(1)
                .withBatchIntervalMs(0)
                .withMaxRetries(3)
                .build(),  // 这个build()很关键
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(timescaleDbUrl)
                    .withDriverName("org.postgresql.Driver")
                    .withUsername(timescaleDbUsername)
                    .withPassword(timescaleDbPassword)
                    .build()
            )
        ).name("Aggregated Data TimescaleDB Sink").setParallelism(1);

        // 6. 启动作业
        env.execute("Flink 加密货币周期价格处理器 - TimescaleDB版本");
    }
    
    /**
     * 针对加密货币周期优化的价格统计聚合函数
     */
    public static class CryptoCyclePriceStatsAggregate implements AggregateFunction<PriceRecord, PriceStats, PriceStats> {
        
        private final int timeDiffThresholdMs;
        
        public CryptoCyclePriceStatsAggregate(int timeDiffThresholdMs) {
            this.timeDiffThresholdMs = timeDiffThresholdMs;
        }

        @Override
        public PriceStats createAccumulator() {
            return new PriceStats();
        }

        @Override
        public PriceStats add(PriceRecord value, PriceStats acc) {
            if (acc.symbol == null) {
                acc.symbol = value.symbol;
            }
            acc.updateWithRecord(value);
            System.out.println("ADD: 币种=" + value.symbol + ", 价格=" + value.last + ", 交易所=" + value.exchange + 
                             ", 当前最高=" + acc.highestPrice + ", 当前最低=" + acc.lowestPrice);
            return acc;
        }

        @Override
        public PriceStats getResult(PriceStats acc) {
            System.out.println("GET_RESULT: 币种=" + acc.symbol + ", 最终最高=" + acc.highestPrice + 
                             ", 最终最低=" + acc.lowestPrice + ", 记录数=" + acc.recordCount);
            return acc;
        }

        @Override
        public PriceStats merge(PriceStats a, PriceStats b) {
            System.out.println("MERGE被调用: A=" + a.symbol + "(" + a.highestPrice + "/" + a.lowestPrice + 
                             "), B=" + b.symbol + "(" + b.highestPrice + "/" + b.lowestPrice + ")");
            
            // 检查输入是否有效
            if (a.symbol == null) {
                return b;
            }
            if (b.symbol == null) {
                return a;
            }

            // 检查时间窗口是否重叠或接近
            // 使用timeDiffThreshold判断是否属于同一时间区间
            long timeDiff = Math.abs(a.earliestTimestamp - b.earliestTimestamp);
            if (timeDiff > timeDiffThresholdMs) {
                // 如果时间差超过阈值，认为是不同时间区间的数据
                // 返回时间较新的那个统计结果
                System.out.println("=== 检测到时间差过大 ===");
                System.out.println("时间差: " + timeDiff + "ms (阈值: " + timeDiffThresholdMs + "ms)");
                System.out.println("币种: " + a.symbol);
                System.out.println("时间范围A: " + formatTimestamp(a.earliestTimestamp) + " - " + formatTimestamp(a.latestTimestamp));
                System.out.println("时间范围B: " + formatTimestamp(b.earliestTimestamp) + " - " + formatTimestamp(b.latestTimestamp));
                System.out.println("选择较新的数据");
                System.out.println("========================");
                return a.latestTimestamp > b.latestTimestamp ? a : b;
            }

            // 合并同一时间窗口内不同交易所的价格数据
            PriceStats merged = new PriceStats(a.symbol);
            
            // 合并最高价格 - 比较不同交易所的价格
            if (a.highestPrice > b.highestPrice) {
                merged.highestPrice = a.highestPrice;
                merged.highestExchange = a.highestExchange;
            } else {
                merged.highestPrice = b.highestPrice;
                merged.highestExchange = b.highestExchange;
            }

            // 合并最低价格 - 比较不同交易所的价格
            if (a.lowestPrice < b.lowestPrice) {
                merged.lowestPrice = a.lowestPrice;
                merged.lowestExchange = a.lowestExchange;
            } else {
                merged.lowestPrice = b.lowestPrice;
                merged.lowestExchange = b.lowestExchange;
            }

            // 合并记录数
            merged.recordCount = a.recordCount + b.recordCount;

            // 合并时间戳范围
            merged.earliestTimestamp = Math.min(a.earliestTimestamp, b.earliestTimestamp);
            merged.latestTimestamp = Math.max(a.latestTimestamp, b.latestTimestamp);

            System.out.println("MERGE结果: 币种=" + merged.symbol + ", 合并后最高=" + merged.highestPrice + 
                             ", 合并后最低=" + merged.lowestPrice + ", 总记录数=" + merged.recordCount);
            return merged;
        }
        
        /**
         * 格式化时间戳为可读格式
         */
        private String formatTimestamp(long timestamp) {
            return new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new java.util.Date(timestamp));
        }
    }
} 