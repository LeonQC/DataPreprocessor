package org.example;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public class PriceWatermarkStrategy {
    
    /**
     * 创建水位线策略，允许最多3秒的延迟（适合10秒币种周期）
     */
    public static WatermarkStrategy<PriceRecord> createWatermarkStrategy() {
        return WatermarkStrategy
                .<PriceRecord>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
    }
    
    /**
     * 创建自定义水位线策略，可以根据需要调整延迟容忍度
     */
    public static WatermarkStrategy<PriceRecord> createCustomWatermarkStrategy(Duration maxOutOfOrderness) {
        return WatermarkStrategy
                .<PriceRecord>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
    }
    
    /**
     * 创建周期性水位线生成器
     */
    public static WatermarkStrategy<PriceRecord> createPeriodicWatermarkStrategy() {
        return WatermarkStrategy
                .<PriceRecord>forGenerator(ctx -> new PeriodicPriceWatermarkGenerator())
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
    }
    
    /**
     * 周期性水位线生成器 - 针对10秒币种周期优化
     */
    public static class PeriodicPriceWatermarkGenerator implements WatermarkGenerator<PriceRecord> {
        private long maxTimestamp = Long.MIN_VALUE;
        private final long maxOutOfOrderness = 3000; // 3秒延迟容忍度（适合10秒周期）
        
        @Override
        public void onEvent(PriceRecord event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }
        
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTimestamp - maxOutOfOrderness));
        }
    }
    
    /**
     * 创建针对币种周期的水位线策略
     * 考虑到每个币种10秒一个周期，不同币种间隔2秒的特点
     */
    public static WatermarkStrategy<PriceRecord> createCryptoCycleWatermarkStrategy() {
        return WatermarkStrategy
                .<PriceRecord>forBoundedOutOfOrderness(Duration.ofSeconds(3)) // 3秒延迟容忍度
                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                .withIdleness(Duration.ofSeconds(15)); // 15秒空闲时间，超过币种周期
    }
} 