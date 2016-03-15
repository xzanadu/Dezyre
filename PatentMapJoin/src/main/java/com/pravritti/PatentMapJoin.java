package com.pravritti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Srinivas Bhagavathula on 3/10/16.
 */
public class PatentMapJoin extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = parseInputOutput(this, getConf(), args);

        job.setNumReduceTasks(0);
        job.setJobName("PatentMapJoin");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(PatentMapJoinMapper.class);

        String fileLocation = "/user/hduser/Dezyre/module5/patentmapjoin/input/list_of_classes.txt";
        URI cacheFileURI = new URI(fileLocation);
        job.addCacheFile(cacheFileURI);
        return job.waitForCompletion(true)? 0 : 1;
    }

    public Job parseInputOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }

        Job job = new Job(conf);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(tool.getClass());
        return job;
    }

    public void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PatentMapJoin(), args);
        System.exit(exitCode);
    }
}
