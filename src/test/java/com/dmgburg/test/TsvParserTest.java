package com.dmgburg.test;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class TsvParserTest {
    private File temp;
    private TsvParser tsvParser;
    private static final String[] line = new String[]{"Col1", "Col2", "Col3", "Col4", "Col5"}; // line length is 25 bytes

    @Before
    public void setup() throws IOException {
        temp = File.createTempFile("temp-file-name", ".tmp");
        tsvParser = new TsvParser(newDirectExecutorService(), 2000);// every line gets new page
    }

    @Test
    public void shouldParseTsv() throws IOException {
        tsvParser = new TsvParser(Executors.newFixedThreadPool(2), 2000);// every line gets new page
        FileOutputStream stream = new FileOutputStream(temp);
        // header
        stream.write(Joiner.on('\t').join(line).concat("\n").getBytes());
        int number = 500000000;
        for (int i = 0; i < number; i++) {
            stream.write(Joiner.on('\t').join(line).concat("\n").getBytes());
        }

        long start = System.currentTimeMillis();
        System.out.println("started");
        Data data = tsvParser.parse(temp);
        System.out.println(number * 25. /((System.currentTimeMillis() - start) / 1000.) + " bytes/s");

        Assert.assertArrayEquals(line, data.getHeaders().toArray());
        Assert.assertEquals(number, data.getData().size());
        Assert.assertArrayEquals(line, data.getData().get(0).toArray());
    }
}
