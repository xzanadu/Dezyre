package com.pravritti;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/4/16.
 */
public class VolPerStockPerMonth {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: VolPerStockPerMonth <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJobName("VolPErStockPerMonth");
        job.setJarByClass(VolPerStockPerMonth.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(VolPerStockPerMonthMapper.class);
        job.setReducerClass(VolPerStockPerMonthReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
