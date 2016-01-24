package org.buldakov.departure;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.Pair;
import org.buldakov.common.TextArrayWritable;

public class TopCarriersByDepPerformanceMapper extends Mapper<Text, Text, Text, TextArrayWritable> {

    private TreeSet<Pair<Double, String>> airlines = new TreeSet<>();

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Double percent = Double.parseDouble(value.toString());
        String origin_carrier = key.toString();
        airlines.add(new Pair<>(percent, origin_carrier));
        if (airlines.size() > 10) {
            airlines.remove(airlines.first());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Pair<Double, String> item : airlines) {
            String[] parts = item.second.split("\t");
            String[] strings = {parts[1], item.first.toString()};
            TextArrayWritable val = new TextArrayWritable(strings);
            context.write(new Text(parts[0]), val);
        }
    }
}