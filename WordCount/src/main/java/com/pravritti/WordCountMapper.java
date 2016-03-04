package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Srinivas Bhagavathula on 2/29/16.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
//        for (String word : line.split("\\s+")) {
//            context.write(new Text(word), new IntWritable(1));
//        }

        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, new IntWritable(1));
        }
    }
}