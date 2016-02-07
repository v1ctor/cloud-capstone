package org.buldakov.batch.task3_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;

public class PopularityMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            context.write(new Text(row.getOrigin()), new IntWritable(1));
            context.write(new Text(row.getDestination()), new IntWritable(1));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
