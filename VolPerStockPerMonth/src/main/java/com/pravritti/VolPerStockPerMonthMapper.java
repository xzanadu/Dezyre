package com.pravritti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/4/16.
 */
public class VolPerStockPerMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");
        String output = "";

        if (!words[0].equalsIgnoreCase("exchange")) {
            output = words[2] + "\t" + words[7];
            if (words[1] != null && !words[1].equalsIgnoreCase("") && words[2] != null && !words[2].equalsIgnoreCase("") &&
                    words[7] != null && !words[7].equalsIgnoreCase(""))
                context.write(new Text(words[1].trim()), new Text(output.trim()));
        }
    }
}
