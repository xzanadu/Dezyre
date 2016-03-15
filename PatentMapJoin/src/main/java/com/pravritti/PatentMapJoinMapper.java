package com.pravritti;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * Created by Srinivas Bhagavathula on 3/10/16.
 */
public class PatentMapJoinMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

    HashMap<Long, String> hm = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {

            URI[] mappingFileURI = context.getCacheFiles();
            BufferedReader br = null;
            FSDataInputStream fsin = null;
            DataInputStream in = null;

            FileSystem fs = FileSystem.get(context.getConfiguration());
            try {
                for (URI uri : mappingFileURI) {
                    Path path = new Path(uri.toString());
                    fsin = fs.open(path);
                    in = new DataInputStream(fsin);
                    br = new BufferedReader(new InputStreamReader(in));

                    String line = br.readLine();
                    while (line != null) {
                        String[] words = line.split("\\t");
                        if (words.length > 0) {
                            if (isNumeric(words[0])) {
                                String cls = words[0];
                                String title = words[1];
                                hm.put(new Long(cls), title);
                            }
                        }
                        line = br.readLine();
                    }
                }
            } finally {
                IOUtils.closeStream(br);
            }
        } else {
            System.out.println(">>>>>> NO CACHE FILES AT ALL");
        }
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(",");

        if (!words[0].equalsIgnoreCase("\"PATENT\"") ) {
            String ptID = words[0];
            String clsID = words[9];

            String clsVal = hm.get(new Long(clsID.trim()));

            context.write(new LongWritable(Long.parseLong(ptID)), new Text(clsVal));
        }
    }

    public boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
        //return str.matches("[-+]?\\d*\\.?\\d+");
    }
}
