package com.decimaltech.csvtodb.config;

import com.decimaltech.csvtodb.model.Account;
import org.springframework.batch.item.ItemProcessor;


// Here we need to create ItemProcessor:
public class AccountItemProcessor implements ItemProcessor<Account, Account> {

    @Override
    public Account process(Account account) throws Exception {
        return account;
    }
}
