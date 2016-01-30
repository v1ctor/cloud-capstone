package org.buldakov.task1_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.buldakov.common.TextArrayWritable;
import org.buldakov.performance.PerformanceReducer;

public class JobRunner {

    //Rank the top 10 airlines by on-time arrival performance.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/task1_2");
        Path resultPath = new Path(args[1]);
        fs.delete(tmpPath, true);
        fs.delete(resultPath, true);

        Job airlinePerformanceJob = Job.getInstance(conf, "Airline performance");

        airlinePerformanceJob.setOutputKeyClass(Text.class);
        airlinePerformanceJob.setOutputValueClass(BooleanWritable.class);

        airlinePerformanceJob.setMapperClass(AirlinePerformanceMapper.class);
        airlinePerformanceJob.setReducerClass(PerformanceReducer.class);

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(airlinePerformanceJob, tmpPath);

        airlinePerformanceJob.setJarByClass(JobRunner.class);
        airlinePerformanceJob.waitForCompletion(true);

        Job top10AirlinePerformance = Job.getInstance(conf, "Top 10 Airline performance");

        top10AirlinePerformance.setOutputKeyClass(Text.class);
        top10AirlinePerformance.setOutputValueClass(DoubleWritable.class);

        top10AirlinePerformance.setMapOutputKeyClass(NullWritable.class);
        top10AirlinePerformance.setMapOutputValueClass(TextArrayWritable.class);

        top10AirlinePerformance.setMapperClass(TopAirlinesPerformanceMapper.class);
        top10AirlinePerformance.setReducerClass(TopAirlinesPerformanceReducer.class);
        top10AirlinePerformance.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(top10AirlinePerformance, tmpPath);
        FileOutputFormat.setOutputPath(top10AirlinePerformance, resultPath);

        top10AirlinePerformance.setInputFormatClass(KeyValueTextInputFormat.class);
        top10AirlinePerformance.setOutputFormatClass(TextOutputFormat.class);

        top10AirlinePerformance.setJarByClass(org.buldakov.task1_3.JobRunner.class);
        System.exit(top10AirlinePerformance.waitForCompletion(true) ? 0 : 1);
    }
}
