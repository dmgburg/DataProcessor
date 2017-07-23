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

    /**
     *
     * Utility method to be used to parse mapping files and create proper Configuration object
     *
     * @param file - input tsv file with key -> value mapping in it
     * @return Map key -> value extracted from file
     * @throws IOException
     */

    public static Map<String, String> parse(File file) throws IOException {
        List<String> configs = Files.readAllLines(file.toPath());
        return configs.stream()
                .map(it -> it.split("\t"))
                .collect(toMap(splitted -> splitted[0], splitted -> splitted[1]));
    }

    public static class Builder {
        private Map<String, String> columnMapping;
        private Map<String, String> rowMapping;

        /**
         *
         * @param columnMapping Map of external column name -> internal column name
         * @return this
         */

        public Builder setColumnMapping(Map<String, String> columnMapping) {
            this.columnMapping = columnMapping;
            return this;
        }

        /**
         *
         * @param rowMapping - Map of external row id -> internal row id
         * @return this
         */

        public Builder setRowMapping(Map<String, String> rowMapping) {
            this.rowMapping = rowMapping;
            return this;
        }

        /**
         * Builds Configura
         * @return Configaration object
         */

        public Configuration build(){
            if (columnMapping == null){
                throw new IllegalStateException("Column mapping not set");
            }
            if (rowMapping == null){
                throw new IllegalStateException("Row mapping not set");
            }
            return new Configuration(columnMapping, rowMapping);
        }
    }
}
