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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class AbstractConnectorTableFunction
        implements ConnectorTableFunction
{
    private final String schema;
    private final String name;
    private final List<ArgumentSpecification> arguments;
    private final ReturnTypeSpecification returnTypeSpecification;

    @JsonCreator
    public AbstractConnectorTableFunction(
            @JsonProperty("schema") String schema,
            @JsonProperty("name") String name,
            @JsonProperty("arguments") List<ArgumentSpecification> arguments,
            @JsonProperty("returnTypeSpecification") ReturnTypeSpecification returnTypeSpecification)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.name = requireNonNull(name, "name is null");
        this.arguments = Collections.unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
        this.returnTypeSpecification = requireNonNull(returnTypeSpecification, "returnTypeSpecification is null");
    }

    @JsonProperty
    @Override
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    @Override
    public String getName()
    {
        return name;
    }

    @JsonProperty
    @Override
    public List<ArgumentSpecification> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    @Override
    public ReturnTypeSpecification getReturnTypeSpecification()
    {
        return returnTypeSpecification;
    }

    @Override
    public abstract TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments);
}
