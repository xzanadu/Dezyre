package com.pravritti;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/5/16.
 */
public class NumberOfPatentsPerYear {
    public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2) {
            System.err.println("Usage: NumberOfPatentsPerYear <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJobName("NumberOfPatentsPerYear");
        job.setJarByClass(NumberOfPatentsPerYear.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NumberOfPatentsPerYearMapper.class);
       // job.setCombinerClass(NumberOfPatentsPerYearReducer.class);
        job.setReducerClass(NumberOfPatentsPerYearReducer.class);


        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
