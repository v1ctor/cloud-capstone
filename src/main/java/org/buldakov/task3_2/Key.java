package org.buldakov.task3_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.joda.time.DateTime;

public class Key implements WritableComparable<Key> {

    private DateTime date;
    private String airport;

    public Key() {
    }

    public Key(DateTime date, String airport) {
        this.date = date;
        this.airport = airport;
    }

    public DateTime getDate() {
        return date;
    }

    public String getAirport() {
        return airport;
    }

    @Override
    public int compareTo(Key o) {
        int i = airport.compareTo(o.airport);
        if (i != 0) {
            return i;
        }
        return date.compareTo(o.date);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(date.toString());
        out.writeUTF(airport);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = DateTime.parse(in.readUTF());
        this.airport = in.readUTF();
    }
}
