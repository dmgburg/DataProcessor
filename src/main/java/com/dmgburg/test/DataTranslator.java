package com.dmgburg.test;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataTranslator {
    private final static Logger log = LoggerFactory.getLogger(DataTranslator.class);
    private static final String readOnlyMode = "r";
    private final static String separator = "\t";
    private final ExecutorService executorService;
    private final int partitionSizeBytes;
    private final Configuration configuration;

    public DataTranslator(Configuration configuration) {
        this(Executors.newSingleThreadExecutor(), 8192, configuration);
    }

    /**
     *
     * @param executorService - thread pool to be used for actual data parsing
     * @param partitionSizeBytes - size of file partitions processed in one job
     * @param configuration - com.dmgburg.test.Configuration object containing column and row mappings
     */



    public DataTranslator(ExecutorService executorService, int partitionSizeBytes, Configuration configuration) {
        this.executorService = executorService;
        this.partitionSizeBytes = partitionSizeBytes;
        this.configuration = configuration;
    }

    /**
     *
     * @param file - File object of file to process
     * @return Data object containing processed data
     * @throws IOException
     */

    public Data parse(final File file) throws IOException {
        log.info("Going to process file {}. size: {}", file.getAbsolutePath(), file.length());
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, readOnlyMode)) {
            Pair<List<String>, BitSet> parsedHeaders = readHeaders(randomAccessFile);
            List<String> mappedHeaders = parsedHeaders.getLeft();
            final BitSet columnFilter = parsedHeaders.getRight();
            List<Future<List<List<String>>>> futures = new ArrayList<>();
            do {
                long start = randomAccessFile.getFilePointer();
                randomAccessFile.skipBytes(partitionSizeBytes);
                scrollToEndOfLine(randomAccessFile);
                long end = randomAccessFile.getFilePointer();
                Future<List<List<String>>> data = executorService.submit(() -> parsePartition(file, start, end, columnFilter));
                futures.add(data);
            } while (randomAccessFile.getFilePointer() < randomAccessFile.length());
            Data.Builder builder = new Data.Builder();
            builder.setHeaders(mappedHeaders);
            for (Future<List<List<String>>> future : futures) {
                try {
                    builder.merge(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    throw new RuntimeException("File parsing failed", e);
                }
            }
            log.info("Processed file {}. Data contains {} rows", file.getAbsolutePath(), builder.getData().size());
            return builder.build();
        }
    }

    private Pair<List<String>, BitSet> readHeaders(RandomAccessFile randomAccessFile) throws IOException {
        String line = randomAccessFile.readLine();
        String[] inboundHeaders = line.split(separator);
        Map<String, String> columnMapping = configuration.getColumnMapping();
        List<String> headers = new ArrayList<>();
        BitSet bitSet = new BitSet(inboundHeaders.length);
        for (int i = 0; i < inboundHeaders.length; i++) {
            String currentHeader = inboundHeaders[i];
            if (columnMapping.containsKey(currentHeader)){
                bitSet.set(i);
                headers.add(columnMapping.get(currentHeader));
            } else {
                bitSet.clear(i);
            }
        }
        return Pair.of(headers, bitSet);
    }

    private void scrollToEndOfLine(RandomAccessFile randomAccessFile) throws IOException {
        while (randomAccessFile.getFilePointer() < randomAccessFile.length() && randomAccessFile.readByte() != '\n') {
            //skip until new line
        }
    }

    private List<List<String>> parsePartition(File file, long partitionStart, long partitionEnd, BitSet columnFilter) throws IOException {
        MappedByteBuffer buffer = null;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, readOnlyMode)) {
            buffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, partitionStart, partitionEnd - partitionStart);
            ArrayList<List<String>> result = new ArrayList<>();
            while (buffer.position() < buffer.capacity()) {
                String[] values = readLine(buffer).split(separator);
                if (!configuration.getRowMapping().containsKey(values[0])){
                    continue;
                }
                List<String> row = new ArrayList<>();
                row.add(configuration.getRowMapping().get(values[0]));
                for (int i = 1; i < values.length; i++) {
                    if (columnFilter.get(i)){
                        row.add(values[i]);
                    }
                }
                result.add(row);
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
