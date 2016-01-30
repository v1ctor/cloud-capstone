package org.buldakov.task2_3;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;

public class ArrivalCarrierPerformanceMapper extends Mapper<Object, Text, Text, BooleanWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            context.write(new Text(row.getOrigin() + "|" + row.getDestination() + "|" + row.getUniqueCarrier()),
                    new BooleanWritable(row.getDepDelay() >= 15));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
