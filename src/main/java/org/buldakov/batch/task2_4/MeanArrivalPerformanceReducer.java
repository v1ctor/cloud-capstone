package org.buldakov.batch.task2_4;

import java.io.IOException;

import com.datastax.driver.core.PreparedStatement;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.CassandraClient;

public class MeanArrivalPerformanceReducer extends Reducer<Text, DoubleWritable, NullWritable, NullWritable> {

    //CREATE TABLE task24 ( origin text, destination text, percent double, PRIMARY KEY(origin, destination));

    private CassandraClient cclient = new CassandraClient();
    private PreparedStatement prepare;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cclient.createConnection("");
        prepare = cclient.getSession().prepare(
                "INSERT INTO capstone.task24 (origin, destination, percent) VALUES (?, ?, ?);");
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double delay = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            delay += val.get();
            count++;
        }

        double meanDelay = delay / count;

        //context.write(key, new DoubleWritable(meanDelay));
        String[] parts = key.toString().split("\\|");
        cclient.execute(prepare.bind(parts[0], parts[1], meanDelay));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        cclient.closeConnection();
    }
}
