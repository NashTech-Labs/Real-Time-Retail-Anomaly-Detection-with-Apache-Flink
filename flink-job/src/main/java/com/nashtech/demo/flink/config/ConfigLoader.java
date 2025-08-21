package com.nashtech.demo.flink.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

public class ConfigLoader {

    private static ConfigLoader instance;
    private Map<String, Object> configMap;

    private ConfigLoader() {
        loadConfig();
    }

    public static ConfigLoader getInstance() {
        if (instance == null) {
            instance = new ConfigLoader();

        }
        return instance;
    }

    private void loadConfig() {
        Yaml yaml = new Yaml();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) throw new RuntimeException("Config file application.yml not found");
            configMap = yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public Object get(String key) {
        String[] parts = key.split("\\.");
        Map<String, Object> current = configMap;
        Object val = null;
        for (int i = 0; i < parts.length; i++) {
            val = current.get(parts[i]);
            if (val == null) return null;
            if (i < parts.length - 1) {
                if (val instanceof Map) {
                    current = (Map<String, Object>) val;
                } else {
                    return null;
                }
            }
        }
        return val;
    }

    public String getString(String key) {
        Object v = get(key);
        return v.toString();
    }

    public int getInt(String key) {
        Object v = get(key);
        return Integer.parseInt(v.toString());
    }

    public long getLong(String key) {
        Object v = get(key);
        return Long.parseLong(v.toString());

    }

    public double getDouble(String key) {
        Object v = get(key);
        return Double.parseDouble(v.toString());
    }
}

