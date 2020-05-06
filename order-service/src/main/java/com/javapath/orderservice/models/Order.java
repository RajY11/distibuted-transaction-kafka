package com.javapath.orderservice.models;



import javax.persistence.*;
import java.util.List;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue
    private int orderId;
    private int customerId;
    private String itemName;  // one order can have multipule Items, but for simplicity i am taking one Item per order.
    private long totalAmount;
    private String status;

    public Order() {
    }

    public Order(int orderId, int customerId, String itemName, long totalAmount, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.itemName = itemName;
        this.totalAmount = totalAmount;
        this.status = status;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public long getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(long totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
