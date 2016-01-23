package org.buldakov.topFromAirports;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.Pair;
import org.buldakov.common.TextArrayWritable;

public class Top10FromAirportsReducer extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
    private TreeSet<Pair<Integer, String>> countToAirports = new TreeSet<>();

    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        for (TextArrayWritable val : values) {
            Text[] pair = (Text[]) val.toArray();
            String word = pair[0].toString();
            Integer count = Integer.parseInt(pair[1].toString());
            countToAirports.add(new Pair<>(count, word));
            if (countToAirports.size() > 10) {
                countToAirports.remove(countToAirports.first());
            }
        }
        for (Pair<Integer, String> item : countToAirports) {
            Text word = new Text(item.second);
            IntWritable value = new IntWritable(item.first);
            context.write(word, value);
        }
    }
}

