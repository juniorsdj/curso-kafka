package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var orderDispatcher = new KafkaDispatcher<Order>();
        var emailDispatcher = new KafkaDispatcher<String>();
        var key = UUID.randomUUID().toString();

        var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
        var order = new Order(userId, orderId, amount);
        orderDispatcher.send("ECOMMERCE_NEW_ORDER", key, order);



        var mail = "Thank you for your order! We are processing your order!";
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", key, mail);

    }

}
