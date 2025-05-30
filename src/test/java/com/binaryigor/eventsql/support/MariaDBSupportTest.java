package com.binaryigor.eventsql.support;

import com.binaryigor.eventsql.EventSQLDialect;

import static com.binaryigor.eventsql.test.IntegrationTestSupport.mariaDBContainer;

public class MariaDBSupportTest extends DBSupportTest {

    public MariaDBSupportTest() {
        super(mariaDBContainer(), EventSQLDialect.MARIADB);
    }
}
