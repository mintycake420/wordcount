package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * For each app, computes average notifications per day.
 * Input:  (app, [n1, n2, ...])
 * Output: (app, avgNotificationsPerDay)
 */
public class AverageNotificationsPerAppReducer
        extends Reducer<Text, IntWritable, Text, FloatWritable> {

    private final FloatWritable outValue = new FloatWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        int count = 0;

        for (IntWritable v : values) {
            sum += v.get();
            count++;
        }

        if (count == 0) {
            return;
        }

        float avg = (float) sum / count;
        outValue.set(avg);
        context.write(key, outValue);
    }
}
