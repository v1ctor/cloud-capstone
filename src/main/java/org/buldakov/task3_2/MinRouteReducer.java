package org.buldakov.task3_2;

import java.io.IOException;
import java.util.TreeMap;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.buldakov.common.CassandraClient;

public class MinRouteReducer extends Reducer<Text, Route, NullWritable, NullWritable> {

    //CREATE TABLE task32 ( origin text, intermediate text, destination text, flightDate text, firstFlight text, secondFlight text,
    // PRIMARY KEY(origin, intermediate, destination, flightDate));

    private CassandraClient cclient = new CassandraClient();
    private PreparedStatement prepare;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cclient.createConnection("");
        prepare = cclient.getSession().prepare(
                "INSERT INTO capstone.task32 (origin, intermediate, destination," +
                        "flightDate, firstFlight, secondFlight) VALUES (?, ?, ?, ?, ?, ?);");
    }

    private TreeMap<Double, Route> routes = new TreeMap<>();

    @Override
    public void reduce(Text key, Iterable<Route> values, Context context) throws IOException, InterruptedException {
        for (Route val: values) {
            routes.put(val.getOverallDelay(), val);
            if (routes.size() > 1) {
                routes.remove(routes.lastKey());
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Route route : routes.values()) {
            BoundStatement statement = prepare.bind(route.getOrigin(), route.getIntermediate(), route.getDestination(),
                    route.getDate().toString(),
                    route.getFirstFlight(), route.getSecondFlight());
            cclient.execute(statement);
        }
        routes = new TreeMap<>();
        cclient.closeConnection();
    }
}
