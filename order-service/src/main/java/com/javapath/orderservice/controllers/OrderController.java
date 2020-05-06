package com.javapath.orderservice.controllers;

import com.javapath.orderservice.models.Order;
import com.javapath.orderservice.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("api/v1")
public class OrderController {

    private OrderService orderService;

    @Autowired
    public OrderController(OrderService orderService){
        this.orderService = orderService;
    }

    @PostMapping("/create")
    public Order createOrder(@RequestBody Order order){
        order = orderService.createOrder(order);
        return order;
    }

    @GetMapping("/order/{orderId}")
    public Optional<Order> findOrder(@PathVariable int orderId){
        return orderService.getOrderById(orderId);
    }
}
