package org.buldakov.week;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

public class WeekDayPerformanceMapper extends Mapper<Object, Text, IntWritable, BooleanWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {

            String[] parts = value.toString().split("\t");

            DateTime date = new DateTime(parts[0]);
            int dayOfWeek = date.dayOfWeek().get();
            double arrDelay = Double.parseDouble(parts[6]);

            context.write(new IntWritable(dayOfWeek), new BooleanWritable(arrDelay >= 15));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
