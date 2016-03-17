package com.pravritti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/16/16.
 */
public class CustomKey implements WritableComparable<CustomKey> {

    private Text cdrID;
    private IntWritable cdrType;

    public CustomKey() {
        this.cdrID = new Text();
        this.cdrType = new IntWritable();
    }

    public CustomKey(Text cdrID, IntWritable cdrType) {
        this.cdrID = cdrID;
        this.cdrType = cdrType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.cdrID.write(dataOutput);
        this.cdrType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.cdrID.readFields(dataInput);
        this.cdrType.readFields(dataInput);

    }

    @Override
    public int compareTo(CustomKey other) {
        if (this.cdrID.compareTo(other.cdrID) != 0) {
            return 1;
        } else
            return this.cdrType.compareTo(other.cdrType);
    }

    public Text getCdrID() {
        return cdrID;
    }

    public IntWritable getCdrType() {
        return cdrType;
    }

    public void setCdrID(Text cdrID) {
        this.cdrID = cdrID;
    }

    public void setCdrType(IntWritable cdrType) {
        this.cdrType = cdrType;
    }
}
