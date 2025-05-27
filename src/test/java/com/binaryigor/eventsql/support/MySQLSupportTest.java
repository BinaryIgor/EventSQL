package com.binaryigor.eventsql.support;

import com.binaryigor.eventsql.EventSQLDialect;

import static com.binaryigor.eventsql.test.IntegrationTestSupport.mySQLContainer;

public class MySQLSupportTest extends DBSupportTest {

    public MySQLSupportTest() {
        super(mySQLContainer(), EventSQLDialect.MYSQL);
    }
}
