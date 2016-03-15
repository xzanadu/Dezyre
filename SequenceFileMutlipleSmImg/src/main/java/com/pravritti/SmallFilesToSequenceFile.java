package com.pravritti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Srinivas Bhagavathula on 3/14/16.
 */

public class SmallFilesToSequenceFile extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SmallFilesToSequenceFile.class);
        job.setJobName("smallfilestoseqfile");
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SmallFilesToSequenceFileMapper.class);

        if (job.waitForCompletion(true)) {
            Configuration cnf = new Configuration();
            Job jb = Job.getInstance(conf);
            jb.setJarByClass(SmallFilesToSequenceFile.class);
            jb.setJobName("removeduplicatefilesequence");
            jb.setInputFormatClass(SequenceFileInputFormat.class);
            jb.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(jb, new Path(args[2]));
            FileOutputFormat.setOutputPath(jb, new Path(args[3]));
            jb.setOutputKeyClass(Text.class);
            jb.setOutputValueClass(Text.class);
            jb.setMapperClass(DuplicateFilesSequenceFileMapper.class);
            jb.setReducerClass(DuplicateFilesSequenceFileReducer.class);
            return jb.waitForCompletion(true) ? 0 : 1;
        } else return 1;
        //return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFile(), args);
        System.exit(exitCode);
    }
}