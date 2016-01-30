package org.buldakov.task2_1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.TextArrayWritable;

public class TopCarriersByDepPerformanceMapper extends Mapper<Text, Text, Text, TextArrayWritable> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Double percent = Double.parseDouble(value.toString());
        String origin_carrier = key.toString();
        String[] parts = origin_carrier.split("\\|");
        String[] strings = {parts[1], percent.toString()};
        TextArrayWritable val = new TextArrayWritable(strings);
        context.write(new Text(parts[0]), val);
    }
}
