package com.pravritti;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Srinivas Bhagavathula on 2/29/16.
 */
public class HighestPriceMonthMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Date date = null;
        String highVol, dtString, highPrice, monthString = null;
        String line = value.toString();

        String[] words = line.split(",");

        if (!(words[0].trim().equalsIgnoreCase("exchange"))) {
            dtString = words[2];
            highPrice = words[4];
            highVol = words[7];

            DateFormat format = new SimpleDateFormat("MM/dd/yy", Locale.ENGLISH);
            try {
                date = format.parse(dtString);

                Format formatter = new SimpleDateFormat("MMMM");
                monthString = formatter.format(date);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            if (date != null && highVol != null && highPrice != null)
                if (Integer.parseInt(highVol) > 300000)
                    context.write(new Text(monthString), new DoubleWritable(Double.parseDouble(highPrice)));
        }
    }
}
