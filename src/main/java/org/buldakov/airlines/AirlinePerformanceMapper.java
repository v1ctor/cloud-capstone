package org.buldakov.airlines;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.TextArrayWritable;

public class AirlinePerformanceMapper extends Mapper<Object, Text, Text, TextArrayWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] parts = value.toString().split("\t");

            String airline = parts[1];
            double arrDelay = Double.parseDouble(parts[6]);

            String[] result = {Boolean.toString(arrDelay >= 15), "1"};
            context.write(new Text(airline), new TextArrayWritable(result));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
