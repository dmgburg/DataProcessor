package com.dmgburg.test;

import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Arrays.asList;

public class DataTranslatorTest {
    private File temp;
    private DataTranslator dataTranslator;
    private static final String[] line = new String[]{"Col1", "Col2", "Col3", "Col4", "Col5"}; // line length is 25 bytes

    @Test
    public void shouldParseTsv() throws IOException {
        temp = File.createTempFile("temp-file-name", ".tmp");
        Map<String, String> columnMapping = createMapping(asList(line), asList(line));

        Map<String, String> rowMapping = createMapping(asList("Col1"), asList("Col1"));

        Configuration configuration = new Configuration.Builder()
                .setColumnMapping(columnMapping)
                .setRowMapping(rowMapping)
                .build();
        dataTranslator = new DataTranslator(newDirectExecutorService(), 2000, configuration);// every line gets new page

        FileOutputStream stream = new FileOutputStream(temp);
        // header
        stream.write(Joiner.on('\t').join(line).concat("\n").getBytes());
        int number = 5;
        for (int i = 0; i < number; i++) {
            stream.write(Joiner.on('\t').join(line).concat("\n").getBytes());
        }

        Data data = dataTranslator.parse(temp);

        Assert.assertArrayEquals(line, data.getHeaders().toArray());
        Assert.assertEquals(number, data.getData().size());
        Assert.assertArrayEquals(line, data.getData().get(0).toArray());
    }

    private Map<String, String> createMapping(List<String> keys, List<String> values) {
        Map<String, String> mapping = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            mapping.put(keys.get(i), values.get(i));
        }
        return mapping;
    }

    @Test
    public void shouldFilterBasedOnConfiguration() throws IOException {
        temp = File.createTempFile("temp-file-name", ".tmp");
        Map<String, String> columnMapping = createMapping(asList("Col1", "Col3", "Col4"), asList("MyCol1", "MyCol3", "MyCol4"));
        Map<String, String> rowMapping = createMapping(asList("Id1"), asList("MyId1"));
        Configuration configuration = new Configuration.Builder()
                .setColumnMapping(columnMapping)
                .setRowMapping(rowMapping)
                .build();

        dataTranslator = new DataTranslator(newDirectExecutorService(), 2000, configuration);// every line gets new page

        FileOutputStream stream = new FileOutputStream(temp);
        // header
        stream.write("Col1\tCol2\tCol3\tCol4\tCol5\n".getBytes()); // header
        stream.write("Id1\tval21\tval31\tval41\tval51\n".getBytes());
        stream.write("Id2\tval22\tval32\tval42\tval52\n".getBytes());

        Data data = dataTranslator.parse(temp);

        Assert.assertArrayEquals(new String[]{"MyCol1", "MyCol3", "MyCol4"}, data.getHeaders().toArray());
        Assert.assertEquals(1, data.getData().size());
        Assert.assertArrayEquals(new String[]{"MyId1", "val31", "val41"}, data.getData().get(0).toArray());
    }
}
