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

    public static void main(String[] args) throws FileNotFoundException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        String path;
        if (args.length == 0) {
            path = "/root/data/csv/cleaned";
        } else {
            path = args[0];
        }

        File root = new File(path);
        for (File file : root.listFiles()) {
            System.out.print("Process: " + file.getAbsolutePath());
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                producer.send(new ProducerRecord<String, String>("onetime_performance", reader.readLine()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
