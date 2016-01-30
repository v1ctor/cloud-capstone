package org.buldakov.task2_1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.TextArrayWritable;

public class TopCarriersByDepPerformanceMapper extends Mapper<Text, Text, Text, TextArrayWritable> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] parts = key.toString().split("\\|");

        String airport = parts[0];
        String airline = parts[1];
        Double percent = Double.parseDouble(value.toString());

        String[] values = {airline, percent.toString()};

        context.write(new Text(airport), new TextArrayWritable(values));
    }
}
