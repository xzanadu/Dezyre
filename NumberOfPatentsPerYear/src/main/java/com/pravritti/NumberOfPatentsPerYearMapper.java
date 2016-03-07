package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/5/16.
 */
public class NumberOfPatentsPerYearMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] valueArray = line.split(",");

        if (!valueArray[0].equalsIgnoreCase("\"PATENT\"") ) {
            int year = Integer.parseInt(valueArray[1]);
            context.write(new IntWritable(year), new IntWritable(1));
        }
    }
}
