package com.pravritti;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Srinivas Bhagavathula on 3/15/16.
 */
public class AccessLogSmlFileSeqMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSystem fs = FileSystem.get(context.getConfiguration());

//        FSDataInputStream fsIn = fs.open(new Path(value.toString()));
//        DataInputStream in = new DataInputStream(fsIn);
//        Scanner sc = new Scanner(in);
//
//        String content = sc.useDelimiter("\\Z").next();
//
//        context.write(value, new BytesWritable(content.getBytes()));


        String uri = value.toString();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
        StringBuffer logFileContent = new StringBuffer();
        String line;
        line = br.readLine();
        while (line != null) {
            logFileContent.append(line + "\n");
            line = br.readLine();
        }
        br.close();
        context.write(value, new BytesWritable(logFileContent.toString()
                .getBytes()));
    }
}
