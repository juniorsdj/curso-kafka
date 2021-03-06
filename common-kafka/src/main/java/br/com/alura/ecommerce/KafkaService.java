package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> extraProperties) {
        this(groupId, parse, type, extraProperties);
        consumer.subscribe(Collections.singletonList(topic));

    }
    KafkaService(String groupId, Pattern pattern, ConsumerFunction parse, Class<T> type, Map<String, String> extraProperties) {
        this(groupId, parse, type, extraProperties);
        consumer.subscribe(pattern);

    }

    private KafkaService(String groupId, ConsumerFunction parse, Class<T> type, Map<String, String> extraProperties){
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, type, extraProperties));
    }



    void run() {
        while (true){
            var recs = consumer.poll(Duration.ofMillis(100));
            if(!recs.isEmpty()){
                for( var rec : recs){
                    parse.consume(rec);
                }
            }

        }
    }


    private Properties getProperties(String groupId, Class<T> type, Map<String,String> overrideProperties){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return  properties;
    }

    @Override
    public void close()  {
        consumer.close();
    }
}
