package org.buldakov.task1_3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.buldakov.performance.PerformanceReducer;

public class JobRunner {

    //Rank the days of the week by on-time arrival performance.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path resultPath = new Path("/capstone/task1_3");
        fs.delete(resultPath, true);

        Job job = Job.getInstance(conf, "Week days performance");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(WeekDayPerformanceMapper.class);
        job.setReducerClass(PerformanceReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/capstone/ontime_input/*.csv"));
        FileOutputFormat.setOutputPath(job, resultPath);

        job.setJarByClass(JobRunner.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}