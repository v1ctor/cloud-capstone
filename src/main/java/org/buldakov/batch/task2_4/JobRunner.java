package org.buldakov.batch.task2_4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class JobRunner {

    //For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job airlinePerformanceJob = Job.getInstance(conf, "Mean arrival delay by Departure-Origin");

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path(args[0]));
        airlinePerformanceJob.setMapOutputKeyClass(Text.class);
        airlinePerformanceJob.setMapOutputValueClass(DoubleWritable.class);

        airlinePerformanceJob.setMapperClass(ArrivalPerformanceMapper.class);
        airlinePerformanceJob.setReducerClass(MeanArrivalPerformanceReducer.class);
        airlinePerformanceJob.setOutputFormatClass(NullOutputFormat.class);

        airlinePerformanceJob.setJarByClass(JobRunner.class);
        System.exit(airlinePerformanceJob.waitForCompletion(true) ? 0 : 1);
    }
}
