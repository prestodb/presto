/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service;

import com.fasterxml.jackson.datatype.hibernate4.Hibernate4Module;
import com.teradata.benchto.service.rest.converters.ZonedDateTimeConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

import static com.fasterxml.jackson.datatype.hibernate4.Hibernate4Module.Feature.USE_TRANSIENT_ANNOTATION;

@EnableScheduling
@EnableRetry
@SpringBootApplication
public class ServiceApp
{
    public static void main(String[] args)
    {
        SpringApplication.run(ServiceApp.class, args);
    }

    @Bean
    public Jackson2ObjectMapperBuilder configureObjectMapper()
    {
        Hibernate4Module hibernate4Module = new Hibernate4Module();
        hibernate4Module.disable(USE_TRANSIENT_ANNOTATION);
        return new Jackson2ObjectMapperBuilder()
                .modulesToInstall(hibernate4Module);
    }

    @Bean
    public ZonedDateTimeConverter zonedDateTimeConverter() {
        return new ZonedDateTimeConverter();
    }
}
