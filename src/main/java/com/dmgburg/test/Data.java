package com.dmgburg.test;

import jdk.nashorn.internal.objects.NativeUint8Array;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Data {
    private final List<String> headers;
    private final List<List<String>> data;

    public Data(List<String> headers, List<List<String>> data) {
        this.headers = headers;
        this.data = data;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<String>> getData() {
        return Collections.unmodifiableList(data);
    }

    public static class Builder {
        private List<String> headers;
        private List<List<String>> data = new ArrayList<>();

        public Builder setHeaders(List<String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder merge(List<List<String>> data) {
            this.data.addAll(data);
            return this;
        }

        public Data build() {
            if (headers == null || headers.isEmpty()){
                throw new IllegalStateException("Header is null or empty");
            }
            return new Data(headers,data);
        }
    }
}
