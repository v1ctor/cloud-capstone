package org.buldakov.task2_4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanArrivalPerformanceReducer extends Reducer<Text, DoubleWritable, Object, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double delay = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            delay += val.get();
            count++;
        }

        double meanDelay = delay / count;

        context.write(key, new DoubleWritable(meanDelay));
    }
}
