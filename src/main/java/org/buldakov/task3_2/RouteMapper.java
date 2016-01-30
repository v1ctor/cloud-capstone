package org.buldakov.task3_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;
import org.joda.time.DateTime;

public class RouteMapper extends Mapper<Object, Text, Key, Flight> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            DateTime date = row.getFlightDate().withTimeAtStartOfDay();
            if (row.isFirstLeg()) {
                //First leg
                context.write(new Key(date.plusDays(2), row.getDestination()),
                        new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getOrigin(), row.getFlight(), date));
            } else {
                //Second leg
                context.write(new Key(date, row.getOrigin()),
                        new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getDestination(), row.getFlight(), date));
            }
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
