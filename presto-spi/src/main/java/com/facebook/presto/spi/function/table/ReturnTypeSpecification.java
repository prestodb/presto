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
package com.facebook.presto.spi.function.table;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The return type declaration refers to the proper columns of the table function.
 * These are the columns produced by the table function as opposed to the columns
 * of input relations passed through by the table function.
 *
 * The return type can be fixed and known at declaration time (DescribedTable),
 * dynamically determined at analysis time (GenericTable), or simply passed through
 * from input tables without adding new columns (OnlyPassThrough).
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = GenericTableReturnTypeSpecification.class, name = "generic_table"),
        @JsonSubTypes.Type(value = OnlyPassThroughReturnTypeSpecification.class, name = "only_pass_through_table"),
        @JsonSubTypes.Type(value = DescribedTableReturnTypeSpecification.class, name = "described_table")})
public abstract class ReturnTypeSpecification
{
    public abstract String getReturnType();
}
