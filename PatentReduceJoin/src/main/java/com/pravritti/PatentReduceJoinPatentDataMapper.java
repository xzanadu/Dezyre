package com.pravritti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/11/16.
 */
public class PatentReduceJoinPatentDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private String ptID;
    private String assigneeID;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(",");

        if (!words[0].equalsIgnoreCase("\"PATENT\"")) {
            ptID = "P\t" + words[0];
            assigneeID = words[6];

            if (assigneeID != null && !assigneeID.equals(""))
                context.write(new LongWritable(Long.parseLong(assigneeID)), new Text(ptID.trim()));
        }
    }
}
