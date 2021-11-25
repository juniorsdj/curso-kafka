package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudDetectorService::parse, Order.class, Map.of());
        service.run();

    }

    void parse(ConsumerRecord<String, Order> record){
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking of fraud");
        System.out.println("key" + record.key()+ " offset "+ record.offset());
        System.out.println("value" + record.value());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }


}
