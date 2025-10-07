package com.sumo.fraud.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Transaction {
    private String userId;
    private double amount;
    private long timestamp;
}
