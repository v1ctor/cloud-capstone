package org.buldakov.batch.task2_1;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;

public class DepartureCarrierPerformanceMapper extends Mapper<Object, Text, Text, BooleanWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            context.write(new Text(row.getOrigin() + "|" + row.getUniqueCarrier()), new BooleanWritable(row.isLateDeparture()));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
