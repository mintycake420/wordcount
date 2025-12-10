package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;

/**
 * Converts Date -> DayOfWeek and outputs (dayOfWeek, TimesOpened).
 */
public class LaunchesByWeekdayMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable();

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

        String dateStr = parts[0].trim();
        String timesStr = parts[4].trim();

        try {
            LocalDate date = LocalDate.parse(dateStr); // expects YYYY-MM-DD
            DayOfWeek dow = date.getDayOfWeek();       // MONDAY, TUESDAY, ...
            String dayName = dow.toString();           // UPPERCASE; fine for output

            int timesOpened = Integer.parseInt(timesStr);

            outKey.set(dayName);
            outValue.set(timesOpened);
            context.write(outKey, outValue);
        } catch (Exception ignored) {
        }
    }
}
