package com.dmgburg.test;

import javax.annotation.concurrent.Immutable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Immutable
public class Configuration {
    private final Map<String, String> columnMapping;
    private final Map<String, String> rowMapping;

    private Configuration(Map<String, String> columnMapping, Map<String, String> rowMapping) {
        this.columnMapping = columnMapping;
        this.rowMapping = rowMapping;
    }

    public Map<String, String> getColumnMapping() {
        return Collections.unmodifiableMap(columnMapping);
    }

    public Map<String, String> getRowMapping() {
        return Collections.unmodifiableMap(rowMapping);
    }

    public static Map<String, String> parse(File file) throws IOException {
        List<String> configs = Files.readAllLines(file.toPath());
        return configs.stream()
                .map(it -> it.split("\t"))
                .collect(toMap(splitted -> splitted[0], splitted -> splitted[1]));
    }

    public static class Builder {
        private Map<String, String> columnMapping;
        private Map<String, String> rowMapping;

        public Builder setColumnMapping(Map<String, String> columnMapping) {
            this.columnMapping = columnMapping;
            return this;
        }

        public Builder setRowMapping(Map<String, String> rowMapping) {
            this.rowMapping = rowMapping;
            return this;
        }

        public Configuration build(){
            return new Configuration(columnMapping, rowMapping);
        }
    }
}
