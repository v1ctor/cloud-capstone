package org.buldakov.topFromAirports;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.common.Pair;
import org.buldakov.common.TextArrayWritable;

public class Top10FromAirportsMapper extends Mapper<Text, Text, NullWritable, TextArrayWritable> {

    private TreeSet<Pair<Integer, String>> countAirportsMap = new TreeSet<>();

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Integer count = Integer.parseInt(value.toString());
        String airport = key.toString();
        countAirportsMap.add(new Pair<>(count, airport));
        if (countAirportsMap.size() > 10) {
            countAirportsMap.remove(countAirportsMap.first());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Pair<Integer, String> item : countAirportsMap) {
            String[] strings = {item.second, item.first.toString()};
            TextArrayWritable val = new TextArrayWritable(strings);
            context.write(NullWritable.get(), val);
        }
    }

}
