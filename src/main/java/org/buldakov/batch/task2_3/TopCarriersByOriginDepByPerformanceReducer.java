package org.buldakov.batch.task2_3;

import java.io.IOException;
import java.util.TreeSet;

import com.datastax.driver.core.PreparedStatement;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.CassandraClient;
import org.buldakov.batch.common.Pair;
import org.buldakov.batch.common.TextArrayWritable;

public class TopCarriersByOriginDepByPerformanceReducer extends Reducer<Text, TextArrayWritable, NullWritable, NullWritable> {

    //CREATE TABLE task23 ( origin text, destination text, airline text, percent double, PRIMARY KEY(origin, destination, airline));

    private CassandraClient cclient = new CassandraClient();
    private PreparedStatement prepare;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cclient.createConnection("");
        prepare = cclient.getSession().prepare(
                "INSERT INTO capstone.task23 (origin, destination, airline, percent) VALUES (?, ?, ?, ?);");
    }

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        TreeSet<Pair<Double, String>> airlines = new TreeSet<>();
        for (TextArrayWritable val: values) {
            Text[] pair = (Text[]) val.toArray();
            String airline = pair[0].toString();
            Double percent = Double.parseDouble(pair[1].toString());
            airlines.add(new Pair<>(percent, airline));
            if (airlines.size() > 10) {
                airlines.remove(airlines.first());
            }
        }
        for (Pair<Double, String> item : airlines) {
            String[] parts = key.toString().split("\\|");
            String origin = parts[0];
            String destination = parts[1];
            String airline = item.second;
            Double percent = item.first;
            //context.write(new Text(key.toString() + "|" + airline), new DoubleWritable(percent));
            cclient.execute(prepare.bind(origin, destination, airline, percent));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        cclient.closeConnection();
    }
}
