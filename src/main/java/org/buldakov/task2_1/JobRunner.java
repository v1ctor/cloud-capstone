package org.buldakov.task2_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.buldakov.common.TextArrayWritable;
import org.buldakov.performance.PerformanceReducer;

public class JobRunner {

    //For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/task2_1");
        fs.delete(tmpPath, true);

        Job airlinePerformanceJob = Job.getInstance(conf, "Departure Carrier performance");

        airlinePerformanceJob.setMapOutputKeyClass(Text.class);
        airlinePerformanceJob.setMapOutputValueClass(BooleanWritable.class);

        airlinePerformanceJob.setMapperClass(DepartureCarrierPerformanceMapper.class);
        airlinePerformanceJob.setReducerClass(PerformanceReducer.class);

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(airlinePerformanceJob, tmpPath);

        airlinePerformanceJob.setJarByClass(JobRunner.class);
        airlinePerformanceJob.waitForCompletion(true);

        Job top10AirlinePerformance = Job.getInstance(conf, "Top Departure Carrier performance");

        FileInputFormat.setInputPaths(top10AirlinePerformance, tmpPath);
        top10AirlinePerformance.setInputFormatClass(KeyValueTextInputFormat.class);
        top10AirlinePerformance.setMapperClass(TopCarriersByDepPerformanceMapper.class);
        top10AirlinePerformance.setMapOutputKeyClass(Text.class);
        top10AirlinePerformance.setMapOutputValueClass(TextArrayWritable.class);

        top10AirlinePerformance.setReducerClass(TopCarriersByDepPerformanceReducer.class);
        top10AirlinePerformance.setOutputFormatClass(NullOutputFormat.class);

        top10AirlinePerformance.setJarByClass(JobRunner.class);
        System.exit(top10AirlinePerformance.waitForCompletion(true) ? 0 : 1);
    }
}
