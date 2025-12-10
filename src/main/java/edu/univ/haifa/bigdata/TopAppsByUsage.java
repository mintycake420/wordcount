package edu.univ.haifa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for: total usage (minutes) per app.
 * Args:
 *   [0] = number of reducers
 *   [1] = input path (CSV file or folder in HDFS)
 *   [2] = output path (folder in HDFS, must NOT exist)
 */
public class TopAppsByUsage extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        int numberOfReducers = Integer.parseInt(args[0]);
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("TopAppsByUsage");
        job.setJarByClass(TopAppsByUsage.class);

        // Input / output paths
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Input / output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper
        job.setMapperClass(TopAppsByUsageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer
        job.setReducerClass(TopAppsByUsageReducer.class);
        job.setOutputKeyClass(Text.class);        // app name
        job.setOutputValueClass(IntWritable.class); // total usage minutes

        job.setNumReduceTasks(numberOfReducers);

        // Submit job and wait
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopAppsByUsage(), args);
        System.exit(res);
    }
}
