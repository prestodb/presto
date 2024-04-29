/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.execution.property;

import com.facebook.airlift.configuration.Config;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This config class corresponds to velox.properties for native execution process. Properties inside will be used in Configs::BaseVeloxQueryConfig in Configs.h/cpp
 */
public class NativeExecutionVeloxConfig
{
    private static final String CODEGEN_ENABLED = "codegen.enabled";
    // Spilling related configs.
    private static final String SPILL_ENABLED = "spill_enabled";
    private static final String AGGREGATION_SPILL_ENABLED = "aggregation_spill_enabled";
    private static final String JOIN_SPILL_ENABLED = "join_spill_enabled";
    private static final String ORDER_BY_SPILL_ENABLED = "order_by_spill_enabled";
    private static final String MAX_SPILL_BYTES = "max_spill_bytes";

    private boolean codegenEnabled;
    private boolean spillEnabled = true;
    private boolean aggregationSpillEnabled = true;
    private boolean joinSpillEnabled = true;
    private boolean orderBySpillEnabled = true;
    // Velox default value is 100GB, as it is designed for Presto cluster
    // use-case. But for presto-on-spark, 500GB is a reasonable default
    private Long maxSpillBytes = 500L << 30;

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        return builder.put(CODEGEN_ENABLED, String.valueOf(getCodegenEnabled()))
                .put(SPILL_ENABLED, String.valueOf(getSpillEnabled()))
                .put(AGGREGATION_SPILL_ENABLED, String.valueOf(getAggregationSpillEnabled()))
                .put(JOIN_SPILL_ENABLED, String.valueOf(getJoinSpillEnabled()))
                .put(ORDER_BY_SPILL_ENABLED, String.valueOf(getOrderBySpillEnabled()))
                .put(MAX_SPILL_BYTES, String.valueOf(getMaxSpillBytes()))
                .build();
    }

    public boolean getCodegenEnabled()
    {
        return codegenEnabled;
    }

    @Config(CODEGEN_ENABLED)
    public NativeExecutionVeloxConfig setCodegenEnabled(boolean codegenEnabled)
    {
        this.codegenEnabled = codegenEnabled;
        return this;
    }

    public boolean getSpillEnabled()
    {
        return spillEnabled;
    }

    @Config(SPILL_ENABLED)
    public NativeExecutionVeloxConfig setSpillEnabled(boolean spillEnabled)
    {
        this.spillEnabled = spillEnabled;
        return this;
    }

    public boolean getAggregationSpillEnabled()
    {
        return aggregationSpillEnabled;
    }

    @Config(AGGREGATION_SPILL_ENABLED)
    public NativeExecutionVeloxConfig setAggregationSpillEnabled(boolean aggregationSpillEnabled)
    {
        this.aggregationSpillEnabled = aggregationSpillEnabled;
        return this;
    }

    public boolean getJoinSpillEnabled()
    {
        return joinSpillEnabled;
    }

    @Config(JOIN_SPILL_ENABLED)
    public NativeExecutionVeloxConfig setJoinSpillEnabled(boolean joinSpillEnabled)
    {
        this.joinSpillEnabled = joinSpillEnabled;
        return this;
    }

    public boolean getOrderBySpillEnabled()
    {
        return orderBySpillEnabled;
    }

    @Config(ORDER_BY_SPILL_ENABLED)
    public NativeExecutionVeloxConfig setOrderBySpillEnabled(boolean orderBySpillEnabled)
    {
        this.orderBySpillEnabled = orderBySpillEnabled;
        return this;
    }

    public Long getMaxSpillBytes()
    {
        return maxSpillBytes;
    }

    @Config(MAX_SPILL_BYTES)
    public NativeExecutionVeloxConfig setMaxSpillBytes(Long maxSpillBytes)
    {
        this.maxSpillBytes = maxSpillBytes;
        return this;
    }
}
