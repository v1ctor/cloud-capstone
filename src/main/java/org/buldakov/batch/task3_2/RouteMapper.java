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
        return (origin.equals("ATL") && destination.equals("LAX") && date.equals(new DateTime("05/04/2008")))
                || (origin.equals("JFK") && destination.equals("MSP") && date.equals(new DateTime("09/09/2008")))
                || (origin.equals("STL") && destination.equals("ORD") && date.equals(new DateTime("26/01/2008")))
                || (origin.equals("MIA") && destination.equals("LAX") && date.equals(new DateTime("18/05/2008")));
    }

    //HACK for queries
    private boolean canBeFirstLeg(String origin, String destination, DateTime date) {
        return (destination.equals("ATL") && origin.equals("BOS") && date.equals(new DateTime("03/04/2008")))
                || (destination.equals("JFK") && origin.equals("PHX") && date.equals(new DateTime("07/09/2008")))
                || (destination.equals("STL") && origin.equals("DFW") && date.equals(new DateTime("24/01/2008")))
                || (destination.equals("MIA") && origin.equals("LAX") && date.equals(new DateTime("16/05/2008")));
    }

}
