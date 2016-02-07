package org.buldakov.batch.task1_3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.buldakov.batch.performance.PerformanceReducer;

public class JobRunner {

    //Rank the days of the week by on-time arrival performance.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path resultPath = new Path(args[1]);
        fs.delete(resultPath, true);

        Job job = Job.getInstance(conf, "Week days performance");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BooleanWritable.class);

        job.setMapperClass(WeekDayPerformanceMapper.class);
        job.setReducerClass(PerformanceReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, resultPath);

        job.setJarByClass(JobRunner.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
