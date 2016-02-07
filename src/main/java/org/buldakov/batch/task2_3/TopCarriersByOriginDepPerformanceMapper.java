package org.buldakov.batch.task2_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.batch.common.TextArrayWritable;

public class TopCarriersByOriginDepPerformanceMapper extends Mapper<Text, Text, Text, TextArrayWritable> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Double percent = Double.parseDouble(value.toString());
        String origin_carrier = key.toString();
        String[] parts = origin_carrier.split("\\|");
        String[] strings = {parts[2], percent.toString()};
        TextArrayWritable val = new TextArrayWritable(strings);
        context.write(new Text(parts[0] + "|" + parts[1]), val);
    }

}
