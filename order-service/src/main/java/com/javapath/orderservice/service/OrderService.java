package com.javapath.orderservice.service;

import com.javapath.orderservice.models.Order;
import com.javapath.orderservice.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class OrderService {

    private OrderRepository orderRepository;
    private KafkaTemplate<String,Order> kafkaTemplate;

    private static final String ORDER_TOPIC = "orderTopic";

    @Autowired
    public OrderService(OrderRepository orderRepository,KafkaTemplate<String,Order> kafkaTemplate){
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Optional<Order> getOrderById(int orderId){
        return orderRepository.findById(orderId);
    }

    public Order createOrder(Order order) {
        orderRepository.save(order);
        sendToOrderTopic(order);
        return order;
    }

    public void updateOrderWithStatus(Order order){
       Optional<Order> newOrder = orderRepository.findById(order.getOrderId());
       if(newOrder!=null){
           order = newOrder.get();
       }
       order.setStatus("PaymentFailed");
       orderRepository.save(order);
    }

    @KafkaListener(topics = "paymentErrorTopic", groupId = "group_json")
    public void paymentFailedListener(Order order){

       updateOrderWithStatus(order);
        //order.setStatus("Payment failed");
        //orderRepository.save(order);
    }

    private void sendToOrderTopic(Order order) {
        kafkaTemplate.send(ORDER_TOPIC,order);
    }

    @Bean
    public StringJsonMessageConverter jsonConverter() {
        return new StringJsonMessageConverter();
    }

}
