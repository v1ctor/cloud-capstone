package org.buldakov.batch.task3_1;

import java.io.IOException;

import com.datastax.driver.core.PreparedStatement;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.CassandraClient;

public class PopularityReducer extends Reducer<Text, IntWritable, NullWritable, NullWritable> {

    //CREATE TABLE task31 ( airport text, flights int, PRIMARY KEY(airport));

    private CassandraClient cclient = new CassandraClient();
    private PreparedStatement prepare;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cclient.createConnection("");
        prepare = cclient.getSession().prepare(
                "INSERT INTO capstone.task31 (airport, flights) VALUES (?, ?);");
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //context.write(key, new IntWritable(sum));
        String airport = key.toString();
        cclient.execute(prepare.bind(airport, sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        cclient.closeConnection();
    }
}
