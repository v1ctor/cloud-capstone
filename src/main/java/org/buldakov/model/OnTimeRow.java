package org.buldakov.model;

import org.joda.time.DateTime;

public class OnTimeRow {

    private final DateTime flightDate;
    private final String uniqueCarrier;
    private final String origin;
    private final String destination;
    private final String crsDepTime;
    private final double depDelay;
    private final double arrDelay;

    public OnTimeRow(DateTime flightDate, String uniqueCarrier, String origin, String destination, String crsDepTime, double depDelay,
            double arrDelay)
    {
        this.flightDate = flightDate;
        this.uniqueCarrier = uniqueCarrier;
        this.origin = origin;
        this.destination = destination;

        this.crsDepTime = crsDepTime;
        this.depDelay = depDelay;
        this.arrDelay = arrDelay;
    }

    public DateTime getFlightDate() {
        return flightDate;
    }

    public String getUniqueCarrier() {
        return uniqueCarrier;
    }

    public String getOrigin() {
        return origin;
    }

    public String getDestination() {
        return destination;
    }

    public String getCrsDepTime() {
        return crsDepTime;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public double getArrDelay() {
        return arrDelay;
    }

    public static OnTimeRow parse(String csv) {
        String[] parts = csv.split("\t");
        return new OnTimeRow(new DateTime(parts[0]), parts[1], parts[2], parts[3], parts[4], Double.parseDouble(parts[5]),
                Double.parseDouble(parts[6]));
    }
}
