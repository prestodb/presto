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
package com.facebook.presto.tvf;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.table.ArgumentSpecification;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JsonBasedTableFunctionMetadata
{
    private final List<ArgumentSpecification> arguments;
    private final ReturnTypeSpecification returnTypeSpecification;
    private final QualifiedObjectName functionName;

    @JsonCreator
    public JsonBasedTableFunctionMetadata(
            @JsonProperty("functionName") QualifiedObjectName functionName,
            @JsonProperty("arguments") List<ArgumentSpecification> arguments,
            @JsonProperty("returnTypeSpecification") ReturnTypeSpecification returnTypeSpecification)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.arguments = Collections.unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
        this.returnTypeSpecification = requireNonNull(returnTypeSpecification, "returnTypeSpecification is null");
    }

    @JsonProperty
    public QualifiedObjectName getQualifiedObjectName()
    {
        return functionName;
    }

    @JsonProperty
    public List<ArgumentSpecification> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public ReturnTypeSpecification getReturnTypeSpecification()
    {
        return returnTypeSpecification;
    }
}
