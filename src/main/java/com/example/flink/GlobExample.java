package com.example.flink;

import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collections;

public class GlobExample {
    public static void main(String... args) throws  Exception {
        File txtFile = new File("/tmp/test/file.txt");
        File csvFile = new File("/tmp/test/file.csv");
        File binFile = new File("/tmp/test/file.bin");

        writeToFile(txtFile, "txt");
        writeToFile(csvFile, "csv");
        writeToFile(binFile, "bin");

        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();
        final TextInputFormat format = new TextInputFormat(new Path("/tmp/test"));

        GlobFilePathFilter filesFilter = new GlobFilePathFilter(
                Collections.singletonList("**"),
                Arrays.asList("**/file.bin")
        );
        System.out.println(Arrays.toString(GlobFilePathFilter.class.getDeclaredFields()));
        format.setFilesFilter(filesFilter);

        DataSet<String> result = env.readFile(format, "/tmp");
        result.writeAsText("/temp/out");
        env.execute("GlobFilePathFilter-Test");
    }

    private static void writeToFile(File fileName, String text) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(text);
        }
    }
}
