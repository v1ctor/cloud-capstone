package org.buldakov.task2_4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;

public class ArrivalPerformanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            context.write(new Text(row.getOrigin() + "|" + row.getDestination()), new DoubleWritable(row.getArrDelay()));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
