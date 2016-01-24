package org.buldakov.departure;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.TextArrayWritable;

public class DepartureCarrierPerformanceMapper extends Mapper<Object, Text, TextArrayWritable, BooleanWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] parts = value.toString().split("\t");

            String origin = parts[2];
            String airline = parts[1];
            double arrDelay = Double.parseDouble(parts[5]); //DepDelay
            String[] result = {origin, airline};
            context.write(new TextArrayWritable(result), new BooleanWritable(arrDelay >= 15));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
