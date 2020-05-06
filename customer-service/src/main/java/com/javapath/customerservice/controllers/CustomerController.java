package com.javapath.customerservice.controllers;

import com.javapath.customerservice.models.Customer;
import com.javapath.customerservice.service.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("customers/api/v1")
public class CustomerController {

    private CustomerService customerService;

    @Autowired
    public CustomerController(CustomerService customerService){
        this.customerService = customerService;
    }

    @PostMapping("/add")
    public Customer addCustomer(@RequestBody Customer customer){
        customer = customerService.addCustomer(customer);
        return customer;
    }

}
