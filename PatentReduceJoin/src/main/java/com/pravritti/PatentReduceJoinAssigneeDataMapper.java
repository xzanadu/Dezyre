package com.pravritti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/11/16.
 */
public class PatentReduceJoinAssigneeDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(",");

        if (!words[0].equalsIgnoreCase("\"ASSIGNEE\"")) {
            String assigneeID = words[0];
            String assigneeDesc = "B\t" + words[1];
            context.write(new LongWritable(Long.parseLong(assigneeID.trim())), new Text(assigneeDesc.trim()));
        }
    }
}
