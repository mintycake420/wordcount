package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Output: key = app, value = "usage,notifications"
 */
public class UsageVsNotificationsMapper
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
        if (parts.length < 4) {
            return;
        }

        String app = parts[1].trim();
        String usageStr = parts[2].trim();
        String notifStr = parts[3].trim();

        outKey.set(app);
        outValue.set(usageStr + "," + notifStr);
        context.write(outKey, outValue);
    }
}
