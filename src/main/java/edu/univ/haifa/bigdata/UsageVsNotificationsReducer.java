package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * For each app, sums total usage and total notifications.
 * Output value format: "totalUsage\ttotalNotifications"
 */
public class UsageVsNotificationsReducer
        extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int totalUsage = 0;
        int totalNotif = 0;

        for (Text v : values) {
            String[] parts = v.toString().split(",");
            if (parts.length != 2) continue;

            try {
                int usage = Integer.parseInt(parts[0].trim());
                int notif = Integer.parseInt(parts[1].trim());
                totalUsage += usage;
                totalNotif += notif;
            } catch (NumberFormatException ignored) {
            }
        }

        outValue.set(totalUsage + "\t" + totalNotif);
        context.write(key, outValue);
    }
}
