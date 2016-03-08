package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/5/16.
 */
public class NumberOfPatentsPerYearReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum = sum + value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
