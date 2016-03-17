package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/16/16.
 */
public class CustomInputFormatSolMapper extends Mapper<CustomKey, CustomValue, Text, IntWritable> {
    @Override
    protected void map(CustomKey key, CustomValue value, Context context) throws IOException, InterruptedException {
        context.write(value.getSMSStatusCode(), new IntWritable(1));
    }
}
