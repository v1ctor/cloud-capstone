package org.buldakov.week;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.TextArrayWritable;

public class WeekDayPerformanceReducer extends Reducer<IntWritable, TextArrayWritable, IntWritable, DoubleWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        int late = 0;
        int count = 0;
        for (TextArrayWritable val : values) {
            Text[] pair = (Text[]) val.toArray();
            late += Boolean.parseBoolean(pair[0].toString()) ? 1 : 0;
            count += Integer.parseInt(pair[1].toString());
        }

        double percentOnTime = 100 - ((late / count) * 100);

        context.write(key, new DoubleWritable(percentOnTime));
    }
}
