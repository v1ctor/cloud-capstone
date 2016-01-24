package org.buldakov.airlines;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.TextArrayWritable;

public class AirlinePerformanceReducer extends Reducer<Text, TextArrayWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        double late = 0;
        double count = 0;
        for (TextArrayWritable val : values) {
            Text[] pair = (Text[]) val.toArray();
            late += Boolean.parseBoolean(pair[0].toString()) ? 1.0 : 0.0;
            count += Double.parseDouble(pair[1].toString());
        }

        double percentOnTime = 100.0 - ((late / count) * 100.0);

        context.write(key, new DoubleWritable(percentOnTime));
    }
}
