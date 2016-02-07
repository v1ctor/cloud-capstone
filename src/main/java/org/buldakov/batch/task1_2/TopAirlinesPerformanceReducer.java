package org.buldakov.batch.task1_2;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.batch.common.Pair;
import org.buldakov.batch.common.TextArrayWritable;

public class TopAirlinesPerformanceReducer extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {

    private TreeSet<Pair<Double, String>> airlines = new TreeSet<>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        for (TextArrayWritable val: values) {
            Text[] pair = (Text[]) val.toArray();
            String airline = pair[0].toString();
            Double percent = Double.parseDouble(pair[1].toString());
            airlines.add(new Pair<>(percent, airline));
            if (airlines.size() > 10) {
                airlines.remove(airlines.first());
            }
        }
        for (Pair<Double, String> item : airlines) {
            Text word = new Text(item.second);
            DoubleWritable value = new DoubleWritable(item.first);
            context.write(word, value);
        }
    }
}
