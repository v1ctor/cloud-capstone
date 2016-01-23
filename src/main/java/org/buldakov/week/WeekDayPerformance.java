package org.buldakov.week;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.buldakov.common.TextArrayWritable;

public class WeekDayPerformance {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Week days performance");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(WeekDayPerformanceMapper.class);
        job.setReducerClass(WeekDayPerformanceReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/capstone/ontime_input/*.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/capstone/week_days_on_time_arrival_performance"));

        job.setJarByClass(WeekDayPerformance.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
