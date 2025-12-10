package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * For each date, finds app with the highest Times Opened.
 * Output: date  "AppName (X times)"
 */
public class DailyTopAppReducer
        extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String bestApp = null;
        int bestTimes = -1;

        for (Text v : values) {
            String[] parts = v.toString().split("\t");
            if (parts.length != 2) {
                continue;
            }

            String app = parts[0];
            int times;
            try {
                times = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                continue;
            }

            if (times > bestTimes) {
                bestTimes = times;
                bestApp = app;
            }
        }

        if (bestApp != null) {
            outValue.set(bestApp + " (" + bestTimes + " times)");
            context.write(key, outValue);
        }
    }
}
