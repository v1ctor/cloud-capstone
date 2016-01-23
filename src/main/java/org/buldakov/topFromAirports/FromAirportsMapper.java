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

            String origin = args[11];
            context.write(new Text(origin), new IntWritable(1));
        }
    }
}

