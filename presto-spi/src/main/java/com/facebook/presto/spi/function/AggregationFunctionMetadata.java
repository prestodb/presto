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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The class contains the extra metadata required for AGGREGATE functions used in {@link SqlInvokedFunction}.
 */
public class AggregationFunctionMetadata
{
    /**
     * intermediate TypeSignature for the aggregation function.
     * This is converted to an intermediate Type using a {@link com.facebook.presto.common.type.TypeManager}
     */
    private final TypeSignature intermediateType;

    /**
     * Determines if the corresponding aggregation function is order-sensitive.
     */
    private final boolean isOrderSensitive;

    @JsonCreator
    public AggregationFunctionMetadata(
            @JsonProperty("intermediateType") TypeSignature intermediateType,
            @JsonProperty("isOrderSensitive") boolean isOrderSensitive)
    {
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.isOrderSensitive = isOrderSensitive;
    }

    @JsonProperty
    public TypeSignature getIntermediateType()
    {
        return intermediateType;
    }

    @JsonProperty
    public boolean isOrderSensitive()
    {
        return isOrderSensitive;
    }

    @Override
    public String toString()
    {
        return format(
                "%s(%s,%s)",
                getClass().getSimpleName(),
                getIntermediateType(),
                isOrderSensitive());
    }
}
