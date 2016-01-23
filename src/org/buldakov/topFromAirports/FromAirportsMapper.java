package org.buldakov.topFromAirports;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FromAirportsMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {

    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String filename = key.toString();
        // We only want to process .txt files
        if (!filename.endsWith(".csv")) {
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(value.getBytes())));
        //Skip headers in csv
        reader.readLine();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] args = line.split(",");
            // 0 - "Year",
            // 1 - "Quarter",
            // 2 - "Month",
            // 3 - "DayofMonth",
            // 4 - "DayOfWeek",
            // 5 - "FlightDate",
            // 6 - "UniqueCarrier",
            // 7 - "AirlineID",
            // 8 - "Carrier",
            // 9 - "TailNum",
            // 10 - "FlightNum",
            // 11 - "Origin",
            // 12 - "OriginCityName",
            // 13 - "OriginState",
            // 14 - "OriginStateFips",
            // 15 - "OriginStateName",
            // 16 - "OriginWac",
            // 17 - "Dest",
            // 18 - "DestCityName",
            // 19 - "DestState",
            // 20 - "DestStateFips",
            // 21 - "DestStateName",
            // 22 - "DestWac",
            // 23 - "CRSDepTime",
            // 24 - "DepTime",
            // 25 - "DepDelay",
            // 26 - "DepDelayMinutes",
            // 27 - "DepDel15",
            // 28 - "DepartureDelayGroups",
            // 29 - "DepTimeBlk",
            // 30 - "TaxiOut",
            // 31 - "WheelsOff",
            // 32 - "WheelsOn",
            // 33 - "TaxiIn",
            // 34 - "CRSArrTime",
            // 35 - "ArrTime",
            // 36 - "ArrDelay",
            // 37 - "ArrDelayMinutes",
            // 38 - "ArrDel15",
            // 39 - "ArrivalDelayGroups",
            // 40 - "ArrTimeBlk",
            // 41 - "Cancelled",
            // 42 - "CancellationCode",
            // 43 - "Diverted",
            // 44 - "CRSElapsedTime",
            // 45 - "ActualElapsedTime",
            // 46 - "AirTime",
            // 47 - "Flights",
            // 48 - "Distance",
            // 49 - "DistanceGroup",
            // 50 - "CarrierDelay",
            // 51 - "WeatherDelay",
            // 52 - "NASDelay",
            // 53 - "SecurityDelay",
            // 54 - "LateAircraftDelay",
            // 55 - "FirstDepTime",
            // 56 - "TotalAddGTime",
            // 57 - "LongestAddGTime",
            // 58 - "DivAirportLandings",
            // 59 - "DivReachedDest",
            // 60 - "DivActualElapsedTime",
            // 61 - "DivArrDelay",
            // 62 - "DivDistance",
            // 63 - "Div1Airport",
            // 64 - "Div1WheelsOn",
            // 65 - "Div1TotalGTime",
            // 66 - "Div1LongestGTime",
            // 67 - "Div1WheelsOff",
            // 68 - "Div1TailNum",
            // 69 - "Div2Airport",
            // 70 - "Div2WheelsOn",
            // 71 - "Div2TotalGTime",
            // 72 - "Div2LongestGTime",
            // 73 - "Div2WheelsOff",
            // 74 - "Div2TailNum",
            String origin = args[11];
            context.write(new Text(origin), new IntWritable(1));
        }
    }
}

