/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.server.ExchangePlanFragmentSource;
import com.facebook.presto.server.TableScanPlanFragmentSource;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TableScanPlanFragmentSource.class, name = "tablescan"),
        @JsonSubTypes.Type(value = ExchangePlanFragmentSource.class, name = "exchange")})
public interface PlanFragmentSource {
}
