package edu.univ.haifa.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for TopAppsByUsage.
 * Input:  one CSV line  -> "Date,App,Usage (minutes),Notifications,Times Opened"
 * Output: (app, usageMinutes)
 */
public class TopAppsByUsageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return; // skip empty lines
        }

        // Skip header line (starts with "Date,App,...")
        if (line.startsWith("Date,")) {
            return;
        }

        String[] parts = line.split(",");

        // Basic sanity check
        if (parts.length < 3) {
            return; // malformed line
        }

        String app = parts[1].trim();
        String usageStr = parts[2].trim();

        try {
            int usageMinutes = Integer.parseInt(usageStr);

            outKey.set(app);
            outValue.set(usageMinutes);
            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // bad number, just skip this record
        }
    }
}
