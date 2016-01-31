package org.buldakov.task3_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RouteReducer extends Reducer<Text, Flight, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Flight> values, Context context) throws IOException, InterruptedException {
        List<Flight> firstLegs = new ArrayList<>();
        List<Flight> secondLegs = new ArrayList<>();
        String airport = key.toString().split("\\|")[1];
        for (Flight flight : values) {
            Flight result = flight.copy();
            if (flight.isFirstLeg()) {
                firstLegs.add(result);
            } else {
                secondLegs.add(result);
            }
        }
        for (Flight firstLeg : firstLegs) {
            for (Flight secondLeg : secondLegs) {
                String resultKey = firstLeg.getAirport() + "|" + airport + "|" + secondLeg.getAirport();

                double overallDelay = firstLeg.getArrDelay() + firstLeg.getDepDelay() + secondLeg.getArrDelay() + secondLeg.getDepDelay();

                Route route = new Route(firstLeg.getAirport(), airport, secondLeg.getAirport(), overallDelay,
                        firstLeg.getDate(),
                        firstLeg.getFlight(), secondLeg.getFlight());
                context.write(new Text(resultKey), new Text(route.toCsv()));
            }
        }
    }
}
