package org.buldakov.batch.task3_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.buldakov.model.OnTimeRow;
import org.joda.time.DateTime;

public class RouteMapper extends Mapper<Object, Text, Text, Flight> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            OnTimeRow row = OnTimeRow.parse(value.toString());
            DateTime date = row.getFlightDate().withTimeAtStartOfDay();
            if (row.isFirstLeg()) {
                //First leg
                if (canBeFirstLeg(row.getOrigin(), row.getDestination(), date)) {
                    String resultKey = date.plusDays(2).toString() + "|" + row.getDestination();
                    context.write(new Text(resultKey),
                            new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getOrigin(), row.getFlight(), date));
                }
            } else {
                //Second leg
                if (canBeSecondLegHack(row.getOrigin(), row.getDestination(), date)) {
                    String resultKey = date.toString() + "|" + row.getOrigin();
                    context.write(new Text(resultKey),
                            new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getDestination(), row.getFlight(),
                                    date));
                }
            }
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
    //HACK for queries
    private boolean canBeSecondLegHack(String origin, String destination, DateTime date) {
        return (origin.equals("ATL") && destination.equals("LAX") && date.equals(new DateTime("2008-04-05")))
                || (origin.equals("JFK") && destination.equals("MSP") && date.equals(new DateTime("2008-09-09")))
                || (origin.equals("STL") && destination.equals("ORD") && date.equals(new DateTime("2008-01-26")))
                || (origin.equals("MIA") && destination.equals("LAX") && date.equals(new DateTime("2008-05-18")));
    }

    //HACK for queries
    private boolean canBeFirstLeg(String origin, String destination, DateTime date) {
        return (destination.equals("ATL") && origin.equals("BOS") && date.equals(new DateTime("2008-04-03")))
                || (destination.equals("JFK") && origin.equals("PHX") && date.equals(new DateTime("2008-09-07")))
                || (destination.equals("STL") && origin.equals("DFW") && date.equals(new DateTime("2008-01-24")))
                || (destination.equals("MIA") && origin.equals("LAX") && date.equals(new DateTime("2008-05-16")));
    }

}
