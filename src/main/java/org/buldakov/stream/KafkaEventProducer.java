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
    private static int limit;
    private static boolean limited;

    public static void main(String[] args) throws FileNotFoundException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        String path;
        limit = 0;
        limited = false;
        if (args.length > 0) {
            path = "/root/data/csv/cleaned";
        } else {
            path = args[0];
        }
        if (args.length == 2) {
            limited = true;
            limit = Integer.parseInt(args[1]);
        }

        File root = new File(path);
        processFiles(root);
    }

    public static void processFiles(File file) {
        if (limited && limit < 0) {
            return;
        }
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
                    if (limited) {
                        limit--;
                        if (limit < 0) {
                            return;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
