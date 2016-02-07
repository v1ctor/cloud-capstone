package org.buldakov.batch.task3_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class JobRunner {

    //Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job airlinePerformanceJob = Job.getInstance(conf, "Airports popularity");

        airlinePerformanceJob.setMapOutputKeyClass(Text.class);
        airlinePerformanceJob.setMapOutputValueClass(IntWritable.class);

        airlinePerformanceJob.setMapperClass(PopularityMapper.class);
        airlinePerformanceJob.setReducerClass(PopularityReducer.class);
        airlinePerformanceJob.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path(args[0]));

        airlinePerformanceJob.setJarByClass(JobRunner.class);
        System.exit(airlinePerformanceJob.waitForCompletion(true) ? 0 : 1);
    }
}
