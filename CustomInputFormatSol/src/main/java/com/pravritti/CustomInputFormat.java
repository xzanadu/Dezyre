package com.pravritti;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/16/16.
 */
public class CustomInputFormat extends FileInputFormat<CustomKey, CustomValue> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        CustomInputRecordReader reader = new CustomInputRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
