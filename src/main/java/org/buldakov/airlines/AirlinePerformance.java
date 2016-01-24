package org.buldakov.airlines;

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
import org.buldakov.week.WeekDayPerformance;

public class AirlinePerformance {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/airline_on_time_arrival_performance");
        fs.delete(tmpPath, true);

        Job airlinePerformanceJob = Job.getInstance(conf, "Airline performance");

        airlinePerformanceJob.setOutputKeyClass(Text.class);
        airlinePerformanceJob.setOutputValueClass(BooleanWritable.class);

        airlinePerformanceJob.setMapperClass(AirlinePerformanceMapper.class);
        airlinePerformanceJob.setReducerClass(PerformanceReducer.class);

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path("/capstone/ontime_input/*.csv"));
        FileOutputFormat.setOutputPath(airlinePerformanceJob, tmpPath);

        airlinePerformanceJob.setJarByClass(AirlinePerformance.class);
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
        FileOutputFormat.setOutputPath(top10AirlinePerformance, new Path("/capstone/top_10_airline_on_time_arrival_performance"));

        top10AirlinePerformance.setInputFormatClass(KeyValueTextInputFormat.class);
        top10AirlinePerformance.setOutputFormatClass(TextOutputFormat.class);

        top10AirlinePerformance.setJarByClass(WeekDayPerformance.class);
        System.exit(top10AirlinePerformance.waitForCompletion(true) ? 0 : 1);
    }
}
