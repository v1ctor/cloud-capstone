package org.buldakov.task3_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class RouteReducer extends Reducer<Text, Flight, Text, Text> {

    private static final Logger LOGGER = Logger.getLogger(RouteReducer.class);

    @Override
    protected void reduce(Text key, Iterable<Flight> values, Context context) throws IOException, InterruptedException {
        List<Flight> firstLegs = new ArrayList<>();
        List<Flight> secondLegs = new ArrayList<>();
        String airport = key.toString().split("\\|")[1];
        for (Flight flight : values) {
            if (flight.isFirstLeg()) {
                firstLegs.add(flight);
            } else {
                secondLegs.add(flight);
            }
        }
        LOGGER.info("Metrics: ke = " + key.toString() + "second = " + secondLegs.size() + " first = " + firstLegs.size());
        for (Flight firstLeg : firstLegs) {
            for (Flight secondLeg : secondLegs) {
                if (!secondLeg.getAirport().equals(firstLeg.getAirport())) {
                    LOGGER.info("match : " + firstLeg.getAirport() + " -> " + airport + " -> " + secondLeg.getAirport());
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
}
