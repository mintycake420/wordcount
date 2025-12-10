package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Input CSV: Date,App,Usage (minutes),Notifications,Times Opened
 * Output: (app, notificationsForThatDay)
 */
public class AverageNotificationsPerAppMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("Date,")) {
            return; // skip empty or header
        }

        String[] parts = line.split(",");
        if (parts.length < 4) {
            return;
        }

        String app = parts[1].trim();
        String notifStr = parts[3].trim();

        try {
            int notifications = Integer.parseInt(notifStr);
            outKey.set(app);
            outValue.set(notifications);
            context.write(outKey, outValue);
        } catch (NumberFormatException ignored) {
        }
    }
}
