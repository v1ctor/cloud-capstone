package org.buldakov.batch.task3_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;

public class Flight implements Writable {

    private boolean firstLeg;
    private double arrDelay;
    private double depDelay;
    private String airport;
    private String flight;
    private DateTime date;

    public Flight() {}

    public Flight(boolean firstLeg, double arrDelay, double depDelay, String airport, String flight, DateTime date) {
        this.firstLeg = firstLeg;
        this.arrDelay = arrDelay;
        this.depDelay = depDelay;
        this.airport = airport;
        this.flight = flight;
        this.date = date;
    }

    public boolean isFirstLeg() {
        return firstLeg;
    }

    public String getAirport() {
        return airport;
    }

    public double getArrDelay() {
        return arrDelay;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public String getFlight() {
        return flight;
    }

    public DateTime getDate() {
        return date;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(firstLeg);
        out.writeDouble(arrDelay);
        out.writeDouble(depDelay);
        out.writeUTF(airport);
        out.writeUTF(flight);
        out.writeUTF(date.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstLeg = in.readBoolean();
        arrDelay = in.readDouble();
        depDelay = in.readDouble();
        airport = in.readUTF();
        flight = in.readUTF();
        date = DateTime.parse(in.readUTF());
    }

    public Flight copy() {
        return new Flight(firstLeg, arrDelay, depDelay, airport, flight, date);
    }
}
