package org.buldakov.task3_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRunner {

//    3.2. Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements:
//    a. The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
//    b. Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
//    c. Tom wants to arrive at each destination with as little delay as possible.
//            Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the flight that satisfies constraints (a) and (b) and has the best on-time performance with respect to constraint (c), if such a flight exists.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/capstone/tmp/task3_2");
        Path resultPath = new Path(args[1]);
        fs.delete(tmpPath, true);
        fs.delete(resultPath, true);

        Job routeFinderJob = Job.getInstance(conf, "Route Finder");

        routeFinderJob.setMapOutputKeyClass(Text.class);
        routeFinderJob.setMapOutputValueClass(BooleanWritable.class);

// TODO
// routeFinderJob.setMapperClass();
//        routeFinderJob.setReducerClass();

        FileInputFormat.setInputPaths(routeFinderJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(routeFinderJob, tmpPath);

        routeFinderJob.setJarByClass(JobRunner.class);
        routeFinderJob.waitForCompletion(true);

        System.exit(routeFinderJob.waitForCompletion(true) ? 0 : 1);
    }
}
