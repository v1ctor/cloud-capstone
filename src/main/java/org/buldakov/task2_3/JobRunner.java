package org.buldakov.task2_3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.buldakov.common.TextArrayWritable;
import org.buldakov.performance.PerformanceReducer;

public class JobRunner {

    //For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/task2_3");
        Path resultPath = new Path("/capstone/task2_3");
        fs.delete(tmpPath, true);
        fs.delete(resultPath, true);

        Job airlinePerformanceJob = Job.getInstance(conf, "Departure-Origin Carrier performance");

        airlinePerformanceJob.setMapOutputKeyClass(Text.class);
        airlinePerformanceJob.setMapOutputValueClass(BooleanWritable.class);

        airlinePerformanceJob.setMapperClass(ArrivalCarrierPerformanceMapper.class);
        airlinePerformanceJob.setReducerClass(PerformanceReducer.class);

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path("/capstone/ontime_input/*.csv"));
        FileOutputFormat.setOutputPath(airlinePerformanceJob, tmpPath);

        airlinePerformanceJob.setJarByClass(JobRunner.class);
        airlinePerformanceJob.waitForCompletion(true);

        Job top10AirlinePerformance = Job.getInstance(conf, "Top Departure-Origin Carrier performance");

        FileInputFormat.setInputPaths(top10AirlinePerformance, tmpPath);
        top10AirlinePerformance.setInputFormatClass(KeyValueTextInputFormat.class);
        top10AirlinePerformance.setMapperClass(TopCarriersByOriginDepPerformanceMapper.class);
        top10AirlinePerformance.setMapOutputKeyClass(Text.class);
        top10AirlinePerformance.setMapOutputValueClass(TextArrayWritable.class);

        top10AirlinePerformance.setReducerClass(TopCarriersByOriginDepByPerformanceReducer.class);
        top10AirlinePerformance.setOutputKeyClass(Text.class);
        top10AirlinePerformance.setOutputValueClass(DoubleWritable.class);
        top10AirlinePerformance.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(top10AirlinePerformance, resultPath);

        top10AirlinePerformance.setJarByClass(JobRunner.class);
        System.exit(top10AirlinePerformance.waitForCompletion(true) ? 0 : 1);
    }
}
