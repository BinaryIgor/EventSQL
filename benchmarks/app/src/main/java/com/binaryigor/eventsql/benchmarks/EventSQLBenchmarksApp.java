package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.EventSQL;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

@SpringBootApplication
@EnableConfigurationProperties(EventsProperties.class)
public class EventSQLBenchmarksApp {

    public static void main(String[] args) {
        SpringApplication.run(EventSQLBenchmarksApp.class, args);
    }

    @Bean
    EventSQL eventSQL(EventsProperties eventsProperties) {
        var dataSources = eventsProperties.dataSources().stream()
                .filter(EventsProperties.DataSourceProperties::enabled)
                .map(this::dataSource)
                .toList();
        return new EventSQL(dataSources, eventsProperties.sqlDialect());
    }

    private DataSource dataSource(EventsProperties.DataSourceProperties properties) {
        var config = new HikariConfig();
        config.setJdbcUrl(properties.url());
        config.setUsername(properties.username());
        config.setPassword(properties.password());
        config.setMinimumIdle(properties.connections());
        config.setMaximumPoolSize(properties.connections());
        return new HikariDataSource(config);
    }
}
