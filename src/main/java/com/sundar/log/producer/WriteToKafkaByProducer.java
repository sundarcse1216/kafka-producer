package com.sundar.log.producer;

import com.sundar.log.bean.LogProperties;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 *
 * @author sundar
 * @since 2018-01-28
 */
public class WriteToKafkaByProducer {

    private final static Logger LOG = Logger.getLogger(KafkaProducer.class);
    private static String TOPIC = null;
    private static String BOOTSTRAP_SERVERS = null;
    private static String CLIENT_ID_CONFIG = null;
    private static String LOG_FILE_PATH = null;
    private static long SLEPP_TIME = 0;

    public WriteToKafkaByProducer() {

        try {
            Properties pro = new Properties();
            pro.load(KafkaProducer.class.getResourceAsStream("/conf/producerconf.properties"));
            WriteToKafkaByProducer.BOOTSTRAP_SERVERS = pro.getProperty("BOOTSTRAP_SERVERS");
            WriteToKafkaByProducer.TOPIC = pro.getProperty("TOPIC");
            WriteToKafkaByProducer.CLIENT_ID_CONFIG = pro.getProperty("CLIENT_ID_CONFIG");
            WriteToKafkaByProducer.LOG_FILE_PATH = pro.getProperty("LOG_FILE_PATH");
            WriteToKafkaByProducer.SLEPP_TIME = Long.parseLong(pro.getProperty("SLEPPING_TIME"));
        } catch (IOException ex) {
            LOG.error("Exception occurred while load Properties : " + ex, ex);
        }
    }

    private static Producer<Long, List<LogProperties>> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    private void ReadLogFile(final String logFilePath) {
        final Producer<Long, List<LogProperties>> producer = createProducer();
        List<LogProperties> propertiesList = null;
        if (logFilePath != null) {
            WriteToKafkaByProducer.LOG_FILE_PATH = logFilePath;
        }
        try {
//            FileInputStream fileReader = new FileInputStream("abc.log");
//            FileReader fileReader = new FileReader("c:/temp/logSample/test.log");
            FileReader fileReader = new FileReader(WriteToKafkaByProducer.LOG_FILE_PATH);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            propertiesList = new ArrayList<>();
            while (true) {
                Boolean isEnd = true;
                String line = bufferedReader.readLine().trim();
                if (line != null && !line.isEmpty()) {
                    isEnd = false;
                    LogProperties properties = new LogProperties();
                    line = line.replaceAll("[", "").replaceAll("]", "");
                    String[] words = line.split(" ");
                    String dateAndTime = words[0].trim() + " " + words[1].trim();
                    String level = words[2].trim();
                    String className = words[3].split(":")[0].trim();
                    String message = words[words.length - 1].trim();
                    properties.setOccurranceDate(dateAndTime);
                    properties.setSeverity(level);
                    properties.setClassName(className);
                    properties.setMessage(message);

                    propertiesList.add(properties);
                    if (propertiesList.size() == 10) {
                        WriteToKafkaByProducer.writeToCluster(producer, propertiesList);
                        propertiesList.clear();
                    }

                } else {
                    if (isEnd && !propertiesList.isEmpty()) {
                        WriteToKafkaByProducer.writeToCluster(producer, propertiesList);
                        propertiesList.clear();
                        break;
                    } else {
                        Thread.sleep(WriteToKafkaByProducer.SLEPP_TIME);
                    }
                }

            }

        } catch (IOException | InterruptedException | ExecutionException ex) {
            LOG.error("Exception occurred while Reading LOG File : " + ex, ex);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static boolean writeToCluster(Producer<Long, List<LogProperties>> producer, List values) throws InterruptedException, ExecutionException {
        final ProducerRecord<Long, List<LogProperties>> record = new ProducerRecord<>(WriteToKafkaByProducer.TOPIC, values);
        RecordMetadata metadata = producer.send(record).get();
        LOG.info("Sent Record : Topic [" + metadata.topic() + "] Partition [" + metadata.partition() + "] Offset ["
                + metadata.offset() + "] TimeStamp [" + metadata.timestamp() + "]");

        return true;
    }

    public static void main(String[] args) {
        try {
            if (args != null && args.length != 0 && !args[0].trim().isEmpty()) {
                new WriteToKafkaByProducer().ReadLogFile(args[0]);
            } else {
                new WriteToKafkaByProducer().ReadLogFile(null);
            }
        } catch (Exception ex) {
            LOG.info("Exception occurred while write to KAFKA : " + ex, ex);
        }
    }
}
