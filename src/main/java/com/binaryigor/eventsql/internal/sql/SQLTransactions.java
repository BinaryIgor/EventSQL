package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.internal.Transactions;
import org.jooq.DSLContext;

public class SQLTransactions implements Transactions, DSLContextProvider {

    private final ThreadLocal<DSLContext> transactionContexts = new ThreadLocal<>();
    private final DSLContext context;

    public SQLTransactions(DSLContext context) {
        this.context = context;
    }

    @Override
    public void execute(Runnable transaction) {
        var tContext = transactionContexts.get();
        if (tContext == null) {
            context.transaction(trx -> {
                try {
                    transactionContexts.set(trx.dsl());
                    transaction.run();
                } finally {
                    transactionContexts.remove();
                }
            });
        } else {
            transaction.run();
        }
    }

    @Override
    public DSLContext get() {
        var tContext = transactionContexts.get();
        return tContext == null ? context : tContext;
    }
}
