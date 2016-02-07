package org.buldakov.batch.task3_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinRouteMapper extends Mapper<Text, Text, Text, Route> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, Route.fromCsv(value.toString()));
    }
}
