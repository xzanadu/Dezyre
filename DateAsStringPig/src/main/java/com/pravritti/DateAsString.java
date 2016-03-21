package com.pravritti;

import org.apache.pig.PrimitiveEvalFunc;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Srinivas Bhagavathula on 3/21/16.
 */
public class DateAsString extends PrimitiveEvalFunc<String, String> {
    @Override
    public String exec(String s) throws IOException {
        try {
            DateFormat df = new SimpleDateFormat("MM/DD/YY");
            Date dt = df.parse(s);
            DateFormat df1 = new  SimpleDateFormat("d MMM, yyyy", Locale.ENGLISH);

            return df1.format(dt);
        } catch(Exception e){
            return "";}
    }
}
