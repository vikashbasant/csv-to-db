package com.decimaltech.csvtodb.model;

import lombok.Data;


@Data
public class Account {
    private String date;
    private String description;
    private String deposits;
    private String withdrawls;
    private String balance;
}
