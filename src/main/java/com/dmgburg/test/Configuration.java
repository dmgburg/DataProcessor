package com.dmgburg.test;

import javax.annotation.concurrent.Immutable;
import java.util.HashMap;

@Immutable
public class Configuration {
    private final HashMap<String, String> columnMapping;
    private final HashMap<String, String> rowMapping;

    public Configuration(HashMap<String, String> columnMapping, HashMap<String, String> rowMapping) {
        this.columnMapping = columnMapping;
        this.rowMapping = rowMapping;
    }
}
