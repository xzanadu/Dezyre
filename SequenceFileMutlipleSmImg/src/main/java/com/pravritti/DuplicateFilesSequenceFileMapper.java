package com.pravritti;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Srinivas Bhagavathula on 3/15/16.
 */
public class DuplicateFilesSequenceFileMapper extends Mapper<Text, BytesWritable, Text, Text> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String md5String = getMD5(value);
        context.write(new Text(md5String), key);
    }


    private String getMD5(BytesWritable value) {
        try {
            byte[] val = value.getBytes();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(val);
            BigInteger number = new BigInteger(1, messageDigest);
            String hashText = number.toString(16);
            // Now we need to zero pad it if you actually want the full 32 chars.
            while (hashText.length() < 32) {
                hashText = "0" + hashText;
            }
            return hashText;
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
