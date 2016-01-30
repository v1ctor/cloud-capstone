package org.buldakov.task2_1;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.Pair;
import org.buldakov.common.TextArrayWritable;

public class TopCarriersDepByPerformanceReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    private TreeSet<Pair<Double, String>> airlines = new TreeSet<>();

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
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
            String[] strings = {item.second, item.first.toString()};
            TextArrayWritable val = new TextArrayWritable(strings);
            context.write(key, val);
        }
    }
}
