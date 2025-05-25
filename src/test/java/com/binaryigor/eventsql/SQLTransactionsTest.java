package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.sql.SQLTransactions;
import com.binaryigor.eventsql.test.IntegrationTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SQLTransactionsTest extends IntegrationTest {

    private final SQLTransactions transactions = new SQLTransactions(dslContext);

    @Test
    void returnsGlobalContextNotInTransaction() {
        assertEquals(dslContext, transactions.get());
    }

    @Test
    void returnsTransactionalContextInTransactions() {
        var txContext = new AtomicReference<DSLContext>();
        var nestedTxContext = new AtomicReference<DSLContext>();
        transactions.execute(() -> {
            txContext.set(transactions.get());
            transactions.execute(() -> {
                nestedTxContext.set(transactions.get());
            });
        });
        assertEquals(dslContext, transactions.get());
        assertNotEquals(dslContext, txContext.get());
        assertEquals(txContext.get(), nestedTxContext.get());
    }
}
