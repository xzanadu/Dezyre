package com.pravritti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Srinivas Bhagavathula on 3/11/16.
 */
public class PatentReduceJoinReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    private ArrayList<Long> patArrayList = new ArrayList<>();
    private String assigneeDesc = "";
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val: values) {
            String value = val.toString();
            String[] valArray = value.split("\\t");
            if (valArray[0].equalsIgnoreCase("P")) {
                String patentID = valArray[1];
                patArrayList.add(new Long(patentID));
            } else {
                assigneeDesc = valArray[1];
            }
        }

        for (Long pID : patArrayList) {
            context.write(new LongWritable(pID), new Text(assigneeDesc));
        }
    }
}
