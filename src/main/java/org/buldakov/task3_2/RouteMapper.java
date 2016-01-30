package org.buldakov.task3_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.buldakov.model.OnTimeRow;
import org.joda.time.DateTime;

public class RouteMapper extends Mapper<Object, Text, Text, Flight> {

    private static final Logger LOGGER = Logger.getLogger(RouteMapper.class);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            DateTime date = row.getFlightDate().withTimeAtStartOfDay();
            if (row.isFirstLeg()) {
                //First leg
                String resultKey = date.plusDays(2).toString() + "|" + row.getDestination();
                LOGGER.info("First Leg Key: " + resultKey);
                context.write(new Text(resultKey),
                        new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getOrigin(), row.getFlight(), date));
            } else {
                //Second leg
                String resultKey = date.toString() + "|" + row.getOrigin();
                LOGGER.info("Second Leg Key: " + resultKey);
                context.write(new Text(resultKey),
                        new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getDestination(), row.getFlight(), date));
            }
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
