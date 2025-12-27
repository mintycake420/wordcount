package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * For each date, sums Times Opened per app, then selects the app with max total.
 * Input values: "AppName\tTimesOpened"
 * Output: date  "AppName (TOTAL times)"
 */
public class DailyTopAppReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // 1) Sum timesOpened per app for this date
        Map<String, Integer> totalsByApp = new HashMap<>();

        for (Text v : values) {
            String[] parts = v.toString().split("\t");
            if (parts.length != 2) continue;

            String app = parts[0].trim();
            int times;
            try {
                times = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                continue;
            }

            totalsByApp.put(app, totalsByApp.getOrDefault(app, 0) + times);
        }

        // 2) Find the app with the maximum total
        String bestApp = null;
        int bestTotal = -1;

        for (Map.Entry<String, Integer> entry : totalsByApp.entrySet()) {
            if (entry.getValue() > bestTotal) {
                bestTotal = entry.getValue();
                bestApp = entry.getKey();
            }
        }

        // 3) Output result
        if (bestApp != null) {
            outValue.set(bestApp + " (" + bestTotal + " times)");
            context.write(key, outValue);
        }
    }
}
