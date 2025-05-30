package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventSQLRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

public class IntegrationTestSupport {

    public static PostgreSQLContainer<?> postgreSQLContainer() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"));
    }

    public static MySQLContainer<?> mySQLContainer() {
        return new MySQLContainer<>(DockerImageName.parse("mysql:9.1"));
    }

    public static MariaDBContainer<?> mariaDBContainer() {
        return new MariaDBContainer<>(DockerImageName.parse("mariadb:10.11"));
    }

    public static DataSource dataSource(JdbcDatabaseContainer<?> dbContainer) {
        var config = new HikariConfig();
        config.setJdbcUrl(dbContainer.getJdbcUrl());
        config.setUsername(dbContainer.getUsername());
        config.setPassword(dbContainer.getPassword());
        return new HikariDataSource(config);
    }

    public static DSLContext dslContext(DataSource dataSource, SQLDialect sqlDialect) {
        return DSL.using(dataSource, sqlDialect);
    }

    public static void cleanDb(DSLContext dslContext, EventSQLRegistry registry) {
        registry.listConsumers().forEach(c -> registry.unregisterConsumer(c.topic(), c.name()));
        registry.listTopics().forEach(t -> registry.unregisterTopic(t.name()));
        dslContext.execute("""
                truncate event_buffer;
                """);

    }
}
