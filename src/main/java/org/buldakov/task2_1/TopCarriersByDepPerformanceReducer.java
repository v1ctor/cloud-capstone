package org.buldakov.task2_1;

import java.io.IOException;
import java.util.TreeSet;

import com.datastax.driver.core.PreparedStatement;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.CassandraClient;
import org.buldakov.common.Pair;
import org.buldakov.common.TextArrayWritable;

public class TopCarriersByDepPerformanceReducer extends Reducer<Text, TextArrayWritable, Text, DoubleWritable> {

    private CassandraClient cclient = new CassandraClient();
    PreparedStatement prepare;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cclient.createConnection("");
        prepare = cclient.getSession().prepare(
                "INSERT INTO capstone.task21 (airport, airline, percent) VALUES (?, ?, ?);");
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
            context.write(new Text(key.toString() + "|" + item.second), new DoubleWritable(item.first));
            cclient.execute(prepare.bind(key.toString(), item.second, item.first));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        cclient.closeConnection();
    }
}
