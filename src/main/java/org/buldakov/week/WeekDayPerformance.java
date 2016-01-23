package org.buldakov.week;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.buldakov.common.TextArrayWritable;
import org.buldakov.topFromAirports.FromAirportsReducer;

public class WeekDayPerformance {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        Job job = Job.getInstance(conf, "Week days performance");

        job.setInputFormatClass(FileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);
        job.setMapperClass(WeekDayPerformanceMapper.class);
        job.setReducerClass(FromAirportsReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/capstone/ontime_input/*.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/capstone/week_days_on_time_arrival_performance"));

        job.setJarByClass(WeekDayPerformance.class);
        job.waitForCompletion(true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
