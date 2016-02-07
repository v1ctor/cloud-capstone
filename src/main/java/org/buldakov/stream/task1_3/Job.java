package org.buldakov.stream.task1_3;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.buldakov.model.OnTimeRow;
import scala.Tuple2;

/**
 * @author Victor Buldakov <vbuldakov@yandex-team.ru>
 */
public class Job {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Airline performance").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        KafkaUtils.createStream(jssc, "localhost", "spark", Collections.singletonMap("ontime_performance", 1))
            .mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Double>> call(Tuple2<String, String> value) throws Exception {
                OnTimeRow row = OnTimeRow.parse(value._2);
                double percent = row.isLateArrival() ? 0.0 : 100.0;
                return new Tuple2<>(Integer.toString(row.getFlightDate().getDayOfWeek()), new Tuple2<>(1, percent));
            }
        }).reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Double> first, Tuple2<Integer, Double> second) throws Exception {
                int count = first._1 + second._1;
                double percent = (first._1 * first._2 + second._1 * second._2) / count;
                return new Tuple2<>(count, percent);
            }
        }).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Double>> value) throws Exception {
                return new Tuple2<>(value._1, value._2._2);
            }
        }).mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> tuple) throws Exception {
                return tuple.swap();
            }
        }).transformToPair(new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
            @Override
            public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> in) throws Exception {
                return in.sortByKey(false);
            }
        }).foreach(new Function<JavaPairRDD<Double, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<Double, String> rdd) throws Exception {
                StringBuilder builder = new StringBuilder();
                for (Tuple2<Double, String> t: rdd.take(10)) {
                    builder.append(t._2).append(" ").append(t._1).append("\n");
                }
                System.out.println(builder.toString());
                return null;
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }
}
