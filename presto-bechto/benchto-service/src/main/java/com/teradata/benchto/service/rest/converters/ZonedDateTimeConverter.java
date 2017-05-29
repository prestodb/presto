/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest.converters;

import org.springframework.core.convert.converter.Converter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ZonedDateTimeConverter
        implements Converter<String, ZonedDateTime>
{
    @Override
    public ZonedDateTime convert(String source)
    {
        return Instant.ofEpochMilli(Long.parseLong(source)).atZone(ZoneId.of("UTC"));
    }
}
