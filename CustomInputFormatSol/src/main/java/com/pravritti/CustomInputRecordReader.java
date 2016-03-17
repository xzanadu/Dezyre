package com.pravritti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/16/16.
 */
public class CustomInputRecordReader extends RecordReader<CustomKey, CustomValue>{

    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processed = false;
    private CustomKey customKey = new CustomKey();
    private CustomValue customValue = new CustomValue();
    private LineReader reader;
    private FSDataInputStream fsdIn;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();

        Path p = fileSplit.getPath();
        FileSystem fs = p.getFileSystem(conf);
        fsdIn = fs.open(p);
        reader = new LineReader(fsdIn, conf);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Text value = new Text();
        System.out.println("Thi is before" + value.toString() + ">>>>");


        reader.readLine(value);
        String line = value.toString();

        if (line != null && !line.equalsIgnoreCase("")) {

            System.out.println("Thi is after" + line + ">>>>");
            String[] values = line.split(";");

            customKey.setCdrID(new Text(values[0]));
            customKey.setCdrType(new IntWritable(Integer.parseInt(values[1])));

            customValue.setPhone1(new Text(values[2]));
            customValue.setPhone2(new Text(values[3]));
            customValue.setSMSStatusCode(new Text(values[4]));
            return true;
        }
        return false;
    }

    @Override
    public CustomKey getCurrentKey() throws IOException, InterruptedException {
        return this.customKey;
    }

    @Override
    public CustomValue getCurrentValue() throws IOException, InterruptedException {
        return this.customValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }


}
