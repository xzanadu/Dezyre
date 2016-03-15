package com.pravritti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/11/16.
 */
public class PatentReduceJoin extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = parseInputOutput(this, getConf(), args);

        job.setJobName("PatentReduceJoin");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PatentReduceJoinReducer.class);

        return job.waitForCompletion(true)? 0 : 1;

    }

    public Job parseInputOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length != 3) {
            printUsage(tool, "<input1> <input2> <output>");
            return null;
        }

        Job job = new Job(conf);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PatentReduceJoinPatentDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PatentReduceJoinAssigneeDataMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setJarByClass(tool.getClass());
        return job;
    }

    public void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PatentReduceJoin(), args);
        System.exit(exitCode);
    }
}
