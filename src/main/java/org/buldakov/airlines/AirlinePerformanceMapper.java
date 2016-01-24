package org.buldakov.airlines;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlinePerformanceMapper extends Mapper<Object, Text, Text, BooleanWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] parts = value.toString().split("\t");

            String airline = parts[1];
            double arrDelay = Double.parseDouble(parts[6]);
            context.write(new Text(airline), new BooleanWritable(arrDelay >= 15));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
