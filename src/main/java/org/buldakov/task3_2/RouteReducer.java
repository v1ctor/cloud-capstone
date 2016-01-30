package org.buldakov.task3_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RouteReducer extends Reducer<Key, Flight, Text, Text> {

    @Override
    protected void reduce(Key key, Iterable<Flight> values, Context context) throws IOException, InterruptedException {
        List<Flight> firstLegs = new ArrayList<>();
        List<Flight> secondLegs = new ArrayList<>();
        for (Flight flight : values) {
            if (flight.isFirstLeg()) {
                firstLegs.add(flight);
            } else {
                secondLegs.add(flight);
            }
        }
        for (Flight firstLeg : firstLegs) {
            for (Flight secondLeg : secondLegs) {
                if (!secondLeg.getAirport().equals(firstLeg.getAirport())) {
                    String resultKey = firstLeg.getAirport() + "|" + key.getAirport() + "|" + secondLeg.getAirport();

                    double overallDelay = firstLeg.getArrDelay() + firstLeg.getDepDelay() + secondLeg.getArrDelay() + secondLeg.getDepDelay();

                    Route route = new Route(firstLeg.getAirport(), key.getAirport(), secondLeg.getAirport(), overallDelay,
                            firstLeg.getDate(),
                            firstLeg.getFlight(), secondLeg.getFlight());
                    context.write(new Text(resultKey), new Text(route.toCsv()));
                }
            }
        }
    }
}
