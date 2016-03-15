package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Srinivas Bhagavathula on 3/7/16.
 */
public class NumberOfPatentsPerYearReducerTest {

    @Test
    public void returnsNumberOfPatentsPerYearRecord() throws IOException {
        new ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>()
                .withReducer(new NumberOfPatentsPerYearReducer())
                .withInput(new IntWritable(1963), Arrays.asList(new IntWritable(1),
                        new IntWritable(1), new IntWritable(1), new IntWritable(1),
                        new IntWritable(1), new IntWritable(1), new IntWritable(1),
                        new IntWritable(1), new IntWritable(1), new IntWritable(1)))
                .withOutput(new IntWritable(1963), new IntWritable(10))
                .runTest();

    }
}
