package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Output: key = date, value = "AppName\tTimesOpened"
 */
public class DailyTopAppMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("Date,")) {
            return;
        }

        String[] parts = line.split(",");
        if (parts.length < 5) {
            return;
        }

        String date = parts[0].trim();
        String app = parts[1].trim();
        String timesStr = parts[4].trim();

        outKey.set(date);
        outValue.set(app + "\t" + timesStr);
        context.write(outKey, outValue);
    }
}
