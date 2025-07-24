package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载器，用于从application.properties文件加载配置
 */
public class ConfigLoader {
    
    private static final Properties properties = new Properties();
    private static boolean loaded = false;
    
    static {
        loadProperties();
    }
    
    private static void loadProperties() {
        try (InputStream input = ConfigLoader.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
                loaded = true;
            } else {
                System.err.println("无法找到application.properties文件，使用默认配置");
            }
        } catch (IOException e) {
            System.err.println("加载配置文件失败: " + e.getMessage());
        }
    }
    
    public static String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
    
    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                System.err.println("配置项 " + key + " 的值 " + value + " 不是有效的整数，使用默认值: " + defaultValue);
            }
        }
        return defaultValue;
    }
    
    public static double getDouble(String key, double defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                System.err.println("配置项 " + key + " 的值 " + value + " 不是有效的浮点数，使用默认值: " + defaultValue);
            }
        }
        return defaultValue;
    }
    
    public static boolean isLoaded() {
        return loaded;
    }
    
    public static void printConfig() {
        System.out.println("=== 当前配置 ===");
        System.out.println("Kafka Topic: " + getString("kafka.topic", "price_topic"));
        System.out.println("Kafka Bootstrap Servers: " + getString("kafka.bootstrap.servers", "localhost:9092"));
        System.out.println("TimescaleDB URL: " + getString("timescaledb.url", "jdbc:postgresql://localhost:5432/crypto_data"));
        System.out.println("TimescaleDB Username: " + getString("timescaledb.username", "postgres"));
        System.out.println("Window Cycle Period: " + getInt("window.cycle.period.seconds", 10) + " seconds");
        System.out.println("==================");
    }
} 