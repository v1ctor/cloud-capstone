package org.buldakov.task3_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;

public class Route implements Writable {

    private String origin;
    private String intermediate;
    private String destination;
    private double overallDelay;
    private DateTime date;
    private String firstFlight;
    private String secondFlight;

    public Route() {
    }

    public String getOrigin() {
        return origin;
    }

    public String getIntermediate() {
        return intermediate;
    }

    public String getDestination() {
        return destination;
    }

    public double getOverallDelay() {
        return overallDelay;
    }

    public DateTime getDate() {
        return date;
    }

    public String getFirstFlight() {
        return firstFlight;
    }

    public String getSecondFlight() {
        return secondFlight;
    }

    public Route(String origin, String intermediate, String destination, double overallDelay, DateTime date, String firstFlight,
            String secondFlight)
    {
        this.origin = origin;
        this.intermediate = intermediate;
        this.destination = destination;
        this.overallDelay = overallDelay;
        this.date = date;
        this.firstFlight = firstFlight;
        this.secondFlight = secondFlight;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(overallDelay);
        out.writeUTF(firstFlight);
        out.writeUTF(secondFlight);
        out.writeUTF(date.toString());
        out.writeUTF(origin);
        out.writeUTF(intermediate);
        out.writeUTF(destination);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        overallDelay = in.readDouble();
        firstFlight = in.readUTF();
        secondFlight = in.readUTF();
        date = DateTime.parse(in.readUTF());
        origin = in.readUTF();
        intermediate = in.readUTF();
        destination = in.readUTF();
    }
}
