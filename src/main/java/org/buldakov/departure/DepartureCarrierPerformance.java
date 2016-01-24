package org.buldakov.departure;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.buldakov.common.TextArrayWritable;

public class DepartureCarrierPerformance {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/departure_carrier_departure_performance");
        fs.delete(tmpPath, true);

        Job airlinePerformanceJob = Job.getInstance(conf, "Departure Carrier performance");

        airlinePerformanceJob.setOutputKeyClass(TextArrayWritable.class);
        airlinePerformanceJob.setOutputValueClass(DoubleWritable.class);

        airlinePerformanceJob.setMapperClass(DepartureCarrierPerformanceMapper.class);
        airlinePerformanceJob.setReducerClass(DeparturePerformanceReducer.class);

        FileInputFormat.setInputPaths(airlinePerformanceJob, new Path("/capstone/ontime_input/*.csv"));
        FileOutputFormat.setOutputPath(airlinePerformanceJob, tmpPath);

        airlinePerformanceJob.setJarByClass(DepartureCarrierPerformance.class);
        airlinePerformanceJob.waitForCompletion(true);

        
        Job top10AirlinePerformance = Job.getInstance(conf, "Top Departure Carrier performance");

        top10AirlinePerformance.setOutputKeyClass(Text.class);
        top10AirlinePerformance.setOutputValueClass(TextArrayWritable.class);

        top10AirlinePerformance.setMapOutputKeyClass(Text.class);
        top10AirlinePerformance.setMapOutputValueClass(TextArrayWritable.class);

        top10AirlinePerformance.setMapperClass(TopCarriersByDepPerformanceMapper.class);
        top10AirlinePerformance.setReducerClass(TopCarriersDepByPerformanceReducer.class);
        top10AirlinePerformance.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(top10AirlinePerformance, tmpPath);
        FileOutputFormat.setOutputPath(top10AirlinePerformance, new Path("/capstone/top_10_carrier_per_origin_departure_performance"));

        top10AirlinePerformance.setOutputFormatClass(TextOutputFormat.class);

        top10AirlinePerformance.setJarByClass(DepartureCarrierPerformance.class);
        System.exit(top10AirlinePerformance.waitForCompletion(true) ? 0 : 1);
    }
}
