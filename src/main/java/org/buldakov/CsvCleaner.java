package org.buldakov;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class CsvCleaner {

    public static void main(String[] args) throws IOException {
        CSVReader reader = null;
        CSVWriter writer = null;
        try {
            File from = new File(args[0]);
            reader = new CSVReader(new FileReader(from));
            writer = new CSVWriter(new FileWriter("Cleaned_" + from.getName()), '\t');
            reader.readNext(); //skip headers
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                List<String> result = new ArrayList<>();
                // 0 - "Year",
                // 1 - "Quarter",
                // 2 - "Month",
                // 3 - "DayofMonth",
                // 4 - "DayOfWeek",
                result.add(nextLine[5]); // 5 - "FlightDate",
                result.add(nextLine[6]); // 6 - "UniqueCarrier",
                // 7 - "AirlineID",
                // 8 - "Carrier",
                // 9 - "TailNum",
                // 10 - "FlightNum",
                result.add(nextLine[11]); // 11 - "Origin",
                // 12 - "OriginCityName",
                // 13 - "OriginState",
                // 14 - "OriginStateFips",
                // 15 - "OriginStateName",
                // 16 - "OriginWac",
                result.add(nextLine[17]); // 17 - "Dest",
                // 18 - "DestCityName",
                // 19 - "DestState",
                // 20 - "DestStateFips",
                // 21 - "DestStateName",
                // 22 - "DestWac",
                result.add(nextLine[23]); // 23 - "CRSDepTime",
                // 24 - "DepTime",
                result.add(nextLine[25]); // 25 - "DepDelay",
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
                result.add(nextLine[36]); // 36 - "ArrDelay",
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
                boolean canceled = !nextLine[41].isEmpty() && Double.parseDouble(nextLine[41]) == 1;
                boolean diverted = !nextLine[43].isEmpty() && Double.parseDouble(nextLine[43]) == 1;
                if (!canceled && !diverted) { //Cancelled
                    writer.writeNext(result.toArray(new String[0]), false);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                writer.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
        System.out.println(args[0] + "; Done!");
    }
}
