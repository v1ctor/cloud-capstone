package org.buldakov.model;

import org.joda.time.DateTime;

public class OnTimeRow {

    private final DateTime flightDate;
    private final String uniqueCarrier;
    private final String flight;
    private final String origin;
    private final String destination;
    private final int crsDepTime;
    private final double depDelay;
    private final double arrDelay;


    public OnTimeRow(DateTime flightDate, String uniqueCarrier, String flight,
            String origin, String destination, int crsDepTime, double depDelay,
            double arrDelay)
    {
        this.flightDate = flightDate;
        this.uniqueCarrier = uniqueCarrier;
        this.origin = origin;
        this.destination = destination;

        this.crsDepTime = crsDepTime;
        this.depDelay = depDelay;
        this.arrDelay = arrDelay;
        this.flight = flight;
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

    public int getCrsDepTime() {
        return crsDepTime;
    }

    public String getFlight() {
        return flight;
    }

    public boolean isFirstLeg() {
        return crsDepTime < 1200;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public boolean isLateDeparture() {
        return depDelay >= 15;
    }

    public double getArrDelay() {
        return arrDelay;
    }

    public boolean isLateArrival() {
        return arrDelay >= 15;
    }

    public static OnTimeRow parse(String csv) {
        String[] parts = csv.split(",");
        return new OnTimeRow(new DateTime(parts[0]),
                parts[1],
                parts[2],
                parts[3],
                parts[4],
                Integer.parseInt(parts[5]),
                Double.parseDouble(parts[6]),
                Double.parseDouble(parts[7]));
    }
}
