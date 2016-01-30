package org.buldakov.task1_3;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;

public class WeekDayPerformanceMapper extends Mapper<Object, Text, Text, BooleanWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            context.write(new Text(Integer.toString(row.getFlightDate().getDayOfWeek())), new BooleanWritable(row.isOnTimeArrival()));
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
