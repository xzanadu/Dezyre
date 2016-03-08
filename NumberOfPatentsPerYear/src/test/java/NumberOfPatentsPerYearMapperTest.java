import com.pravritti.NumberOfPatentsPerYearMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/7/16.
 */
public class NumberOfPatentsPerYearMapperTest {

    @Test
    public void processValidRecord() throws IOException {
        Text value = new Text("3114597,1963,1446,,\"US\",\"WA\",,2,,264,5,51,,9,,0.7901,,,,,,,");

        new MapDriver<LongWritable, Text, IntWritable, IntWritable>()
                .withMapper(new NumberOfPatentsPerYearMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new IntWritable(1963), new IntWritable(1))
                .runTest();
    }
}
