package org.buldakov.topFromAirports;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.buldakov.common.TextArrayWritable;
import org.buldakov.common.ZipFileInputFormat;

public class Top10FromAirports {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/top_10_from_airports");
        fs.delete(tmpPath, true);
        Job jobA = Job.getInstance(conf, "Airports Count");
        jobA.setInputFormatClass(ZipFileInputFormat.class);
        jobA.setOutputFormatClass(TextOutputFormat.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapperClass(FromAirportsMapper.class);
        jobA.setReducerClass(FromAirportsReducer.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        ZipFileInputFormat.setLenient(true);
        ZipFileInputFormat.setInputPaths(jobA, new Path("/capstone/ontime_input/*.zip"));
        TextOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(Top10FromAirports.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airports");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);
        jobB.setMapperClass(Top10FromAirportsMapper.class);
        jobB.setReducerClass(Top10FromAirportsReducer.class);
        jobB.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        jobB.setJarByClass(Top10FromAirports.class);
        System.exit(jobB.waitForCompletion(true) ? 0 : 1);
    }
}
