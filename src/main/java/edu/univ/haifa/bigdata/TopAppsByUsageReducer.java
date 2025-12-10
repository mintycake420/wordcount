package edu.univ.haifa.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for TopAppsByUsage.
 * For each app, sums all usage minutes.
 * Input:  (app, [usage1, usage2, ...])
 * Output: (app, totalUsageMinutes)
 */
public class TopAppsByUsageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }

        outValue.set(sum);
        context.write(key, outValue);
    }
}
