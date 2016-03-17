package com.pravritti;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/16/16.
 */
public class CustomValue implements WritableComparable<CustomValue>{

    private Text phone2, phone1, SMSStatusCode;

    public CustomValue() {
        phone1 = new Text();
        phone2 = new Text();
        SMSStatusCode = new Text();
    }

    public CustomValue(Text ph1, Text ph2, Text stCode) {
        phone1 = ph1;
        phone2 = ph2;
        SMSStatusCode = stCode;
    }

    @Override
    public int compareTo(CustomValue other) {
        if (this.phone1.compareTo(other.phone1) != 0) {
            return 1;
        } else if (this.phone2.compareTo(other.phone2) != 0) {
            return 1;
        } else return this.SMSStatusCode.compareTo(other.SMSStatusCode);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    public Text getPhone2() {
        return phone2;
    }

    public Text getPhone1() {
        return phone1;
    }

    public Text getSMSStatusCode() {
        return SMSStatusCode;
    }

    public void setPhone2(Text phone2) {
        this.phone2 = phone2;
    }

    public void setPhone1(Text phone1) {
        this.phone1 = phone1;
    }

    public void setSMSStatusCode(Text SMSStatusCode) {
        this.SMSStatusCode = SMSStatusCode;
    }
}
