package org.buldakov.task2_1;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeparturePerformanceReducer extends Reducer<Text, BooleanWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
        double late = 0;
        int count = 0;
        for (BooleanWritable val : values) {
            late += val.get() ? 1.0 : 0.0;
            count++;
        }

        double percentOnTime = 100.0 - ((late / count) * 100.0);

        context.write(key, new DoubleWritable(percentOnTime));
    }
}
