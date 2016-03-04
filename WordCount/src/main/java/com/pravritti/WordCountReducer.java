package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 2/22/16.
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int numOfWords = 0;
        for (IntWritable value: values) {
            numOfWords += value.get();
        }
        context.write(key, new IntWritable(numOfWords));
    }
}
