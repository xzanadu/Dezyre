package com.pravritti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 2/29/16.
 */
public class StockVolumeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private String stkSym, stkVol;
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] values = line.split(",");

        if (!(values[0].trim().equalsIgnoreCase("exchange"))) {
            stkSym = values[1];
            stkVol = values[7];
            context.write(new Text(stkSym), new LongWritable(Long.parseLong(stkVol)));
        }
    }
}
