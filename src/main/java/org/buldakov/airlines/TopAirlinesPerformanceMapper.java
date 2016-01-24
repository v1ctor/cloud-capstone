package org.buldakov.airlines;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.Pair;
import org.buldakov.common.TextArrayWritable;

public class TopAirlinesPerformanceMapper extends Mapper<Text, Text, NullWritable, TextArrayWritable> {

    private TreeSet<Pair<Double, String>> airlines = new TreeSet<>();

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Double percent = Double.parseDouble(value.toString());
        String airline = key.toString();
        airlines.add(new Pair<>(percent, airline));
        if (airlines.size() > 10) {
            airlines.remove(airlines.first());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Pair<Double, String> item : airlines) {
            String[] strings = {item.second, item.first.toString()};
            TextArrayWritable val = new TextArrayWritable(strings);
            context.write(NullWritable.get(), val);
        }
    }
}
