package org.buldakov.week;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.TextArrayWritable;
import org.joda.time.DateTime;

public class WeekDayPerformanceMapper extends Mapper<Text, Text, IntWritable, TextArrayWritable> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        DateTime date = new DateTime(parts[0]);
        int dayOfWeek = date.dayOfWeek().get();
        double arrDelay = Double.parseDouble(parts[6]);

        String[] result = {Boolean.toString(arrDelay >= 15), "1"};
        context.write(new IntWritable(dayOfWeek), new TextArrayWritable(result));
    }
}
