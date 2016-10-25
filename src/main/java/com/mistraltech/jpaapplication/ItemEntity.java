package com.mistraltech.jpaapplication;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class ItemEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private int value;

    public ItemEntity() {
    }

    public Long getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    public void incrementValue() {
        value++;
    }

    @Override
    public String toString() {
        return String.format("ItemEntity: {id = %d, value = %d}", id, value);
    }
}
