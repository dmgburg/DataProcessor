package com.dmgburg.test;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TsvParser {
    private final static Logger log = LoggerFactory.getLogger(TsvParser.class);
    private static final String readOnlyMode = "r";
    private final static String separator = "\t";
    private final ExecutorService executorService;
    private final int partitionSizeBytes;

    public TsvParser(ExecutorService executorService, int partitionSizeBytes) {
        this.executorService = executorService;
        this.partitionSizeBytes = partitionSizeBytes;
    }

    public Data parse(final File file) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, readOnlyMode)) {
            List<String> headers = readHeaders(randomAccessFile);
            List<Future<List<List<String>>>> futures = new ArrayList<>();
            do {
                long start = randomAccessFile.getFilePointer();
                randomAccessFile.skipBytes(partitionSizeBytes);
                scrollToEndOfLine(randomAccessFile);
                long end = randomAccessFile.getFilePointer();
                Future<List<List<String>>> data = executorService.submit(() -> parsePartition(file, start, end));
                futures.add(data);
            } while (randomAccessFile.getFilePointer() < randomAccessFile.length());
            Data.Builder builder = new Data.Builder();
            builder.setHeaders(headers);
            for (Future<List<List<String>>> future : futures) {
                try {
                    builder.merge(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    throw new RuntimeException("File parsing failed", e);
                }
            }
            return builder.build();
        }
    }

    private List<String> readHeaders(RandomAccessFile randomAccessFile) throws IOException {
        String line = randomAccessFile.readLine();
        return Arrays.asList(line.split(separator));
    }

    private void scrollToEndOfLine(RandomAccessFile randomAccessFile) throws IOException {
        while (randomAccessFile.getFilePointer() < randomAccessFile.length() && randomAccessFile.readByte() != '\n') {
            //skip until new line
        }
    }

    private List<List<String>> parsePartition(File file, long partitionStart, long partitionEnd) throws IOException {
        MappedByteBuffer buffer = null;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, readOnlyMode)) {
            buffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, partitionStart, partitionEnd - partitionStart);
            ArrayList<List<String>> result = new ArrayList<>();
            while (buffer.position() < buffer.capacity()) {
                String line = readLine(buffer);
                result.add(Arrays.asList(line.split(separator)));
            }
            return result;
        } finally {
            if (buffer != null) {
                buffer.clear();
            }
        }
    }

    private String readLine(MappedByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        char ch;
        do {
            ch =(char) buffer.get();
            if ( ch == '\n'){
                break;
            }
            sb.append(ch);
        } while (buffer.position() < buffer.capacity());
        return sb.toString();
    }


}
