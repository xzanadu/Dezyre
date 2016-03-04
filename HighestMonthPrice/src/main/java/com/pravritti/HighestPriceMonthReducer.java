package com.pravritti;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 2/29/16.
 */
public class HighestPriceMonthReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double maxPrice = -1.00;
        for (DoubleWritable value : values) {
            maxPrice = Math.max(maxPrice, value.get());
        }
        context.write(key, new DoubleWritable(maxPrice));
    }
}
