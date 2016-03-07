package com.pravritti;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by Srinivas Bhagavathula on 3/4/16.
 */
public class VolPerStockPerMonthReducer extends Reducer<Text, Text, Text, Text> {
    private String[] itemArray = null;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String symbol = "";
        HashMap<String, Long> hm = new HashMap<>();
        for (Text value : values) {
            String line = value.toString();
            itemArray = line.split("\t");
            String monthString = this.getMonthFromDateString(itemArray[0].trim());
            if (!hm.containsKey(monthString)) {
                hm.put(monthString, Long.parseLong(itemArray[1]));
            } else {
                long monthVol = hm.get(monthString);
                monthVol += Long.parseLong(itemArray[1]);
                hm.replace(monthString, monthVol);
            }
        }
        String outputString = "";
        double totalVol = 0.0;
        for (Map.Entry<String,Long> entry : hm.entrySet()) {
            outputString += (entry.getKey() + " " + entry.getValue().toString()) + " ";
            totalVol += entry.getValue();
        }
        outputString += totalVol;

        context.write(key, new Text(outputString));
    }

    private String getMonthFromDateString(String dateString) {

        Date date = new Date(dateString);
        SimpleDateFormat sDF = new SimpleDateFormat("MMMMM", Locale.US);
        String monthString = sDF.format(date);
        return monthString;
    }

}
