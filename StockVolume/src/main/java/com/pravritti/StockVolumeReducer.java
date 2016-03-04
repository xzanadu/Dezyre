package com.pravritti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 2/29/16.
 */
public class StockVolumeReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value: values){
                sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
