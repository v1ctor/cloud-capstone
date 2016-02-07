package org.buldakov.stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author Victor Buldakov <vbuldakov@yandex-team.ru>
 */
public class KafkaEventProducer {

    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) throws FileNotFoundException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        String path;
        if (args.length == 0) {
            path = "/root/data/csv/cleaned";
        } else {
            path = args[0];
        }

        File root = new File(path);
        processFiles(root);
    }

    public static void processFiles(File file) {
        if (file.isDirectory()) {
            for (File next : file.listFiles()) {
                processFiles(next);
            }
        } else {
            System.out.println("Process: " + file.getAbsolutePath());
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    producer.send(new ProducerRecord<String, String>("ontime_performance", line));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
