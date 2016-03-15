package com.pravritti;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/15/16.
 */
public class DuplicateFilesSequenceFileReducer extends Reducer<Text, Text, Text, Text > {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val: values) {
            String value = val.toString();
            context.write(val, key);
            break;
        }
    }
}
