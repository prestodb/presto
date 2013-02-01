/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TableScanPlanFragmentSource.class, name = "tablescan"),
        @JsonSubTypes.Type(value = ExchangePlanFragmentSource.class, name = "exchange")})
public interface PlanFragmentSource {
}
