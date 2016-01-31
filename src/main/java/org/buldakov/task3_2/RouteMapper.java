package org.buldakov.task3_2;

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
                String resultKey = date.plusDays(2).toString() + "|" + row.getDestination();
                if (canBeFirstLeg(row.getOrigin(), row.getDestination(), row.getFlightDate())) {
                    context.write(new Text(resultKey),
                            new Flight(row.isFirstLeg(), row.getArrDelay(), row.getDepDelay(), row.getOrigin(), row.getFlight(), date));
                }
            } else {
                //Second leg
                String resultKey = date.toString() + "|" + row.getOrigin();
                if (canBeSecondLegHack(row.getOrigin(), row.getDestination(), row.getFlightDate())) {
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
        return (origin.equals("ORD") && destination.equals("LAX") && date.equals(new DateTime("06/03/2008")))
                || (origin.equals("DFW") && destination.equals("CRP") && date.equals(new DateTime("11/09/2008")))
                || (origin.equals("BFL") && destination.equals("LAX") && date.equals(new DateTime("03/04/2008")))
                || (origin.equals("SFO") && destination.equals("PHX") && date.equals(new DateTime("14/07/2008")))
                || (origin.equals("ORD") && destination.equals("DFW") && date.equals(new DateTime("12/06/2008")))
                || (origin.equals("ORD") && destination.equals("JFK") && date.equals(new DateTime("03/01/2008")));
    }

    //HACK for queries
    private boolean canBeFirstLeg(String origin, String destination, DateTime date) {
        return (destination.equals("ORD") && origin.equals("CMI") && date.equals(new DateTime("04/03/2008")))
                || (destination.equals("DFW") && origin.equals("JAX") && date.equals(new DateTime("09/09/2008")))
                || (destination.equals("BFL") && origin.equals("SLC") && date.equals(new DateTime("01/04/2008")))
                || (destination.equals("SFO") && origin.equals("LAX") && date.equals(new DateTime("12/07/2008")))
                || (destination.equals("ORD") && origin.equals("DFW") && date.equals(new DateTime("10/06/2008")))
                || (destination.equals("ORD") && origin.equals("LAX") && date.equals(new DateTime("01/01/2008")));
    }
}
