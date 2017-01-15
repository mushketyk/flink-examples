package com.example.flink;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class StanfordTweetsDataSetInputFormat extends RichInputFormat<TwitterFollower, TweetFileInputSplit> {

    private static final Logger logger = LoggerFactory.getLogger(StanfordTweetsDataSetInputFormat.class);
    private final String path;
    private transient BufferedReader reader;
    private String nextLine;

    public StanfordTweetsDataSetInputFormat(String path) {
        try {
            logger.info("Creating input format");
            this.path = path;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a file system", e);
        }
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public TweetFileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        FileSystem fileSystem = createFileSystem();
        FileStatus[] statuses = fileSystem.listStatus(new Path(path));
        logger.info("Found {} files", statuses.length);

        List<TweetFileInputSplit> splits = new ArrayList<>();
        for (int i = 0; i < statuses.length; i++) {
            FileStatus status = statuses[i];
            String fileName = status.getPath().getName();
            if (fileName.endsWith("edges")) {
                splits.add(new TweetFileInputSplit(i, status.getPath()));
            }
        }

        logger.info("Result number of splits: {}", splits.size());
        return splits.toArray(new TweetFileInputSplit[splits.size()]);
    }

    private FileSystem createFileSystem() throws IOException {
        try {
            return FileSystem.get(new URI(path));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(TweetFileInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(TweetFileInputSplit split) throws IOException {
        if (reader != null) {
            reader.close();
        }
        //
        FileSystem fileSystem = createFileSystem();
        this.reader = new BufferedReader(new InputStreamReader(fileSystem.open(split.getPath())));
        // Pre-read next line to easily check if we've reached the end of an input split
        this.nextLine = reader.readLine();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return nextLine == null;
    }

    @Override
    public TwitterFollower nextRecord(TwitterFollower reuse) throws IOException {
        String[] split = nextLine.split(" ");
        int userId = Integer.parseInt(split[0]);
        int followerId = Integer.parseInt(split[1]);

        reuse.setUser(userId);
        reuse.setFollower(followerId);
        nextLine = reader.readLine();

        return reuse;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}

class TweetFileInputSplit implements InputSplit {

    private final int splitNumber;
    private final Path path;

    public TweetFileInputSplit(int splitNumber, Path path) {
        this.splitNumber = splitNumber;
        this.path = path;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    public Path getPath() {
        return path;
    }
}
