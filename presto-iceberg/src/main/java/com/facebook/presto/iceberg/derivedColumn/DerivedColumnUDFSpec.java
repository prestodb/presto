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
package com.facebook.presto.iceberg.derivedColumn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

public class DerivedColumnUDFSpec
{
    private final String catalog;
    private final String schema;
    private final String functionName;
    private final List<String> parameterTypes;
    private final List<DerivedColumnArgumentSpec> arguments;
    private final String derivedColumnName;
    private final String returnType;

    @JsonCreator
    public DerivedColumnUDFSpec(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("functionName") String functionName,
            @JsonProperty("params") List<String> parameterTypes,
            @JsonProperty("arguments") List<DerivedColumnArgumentSpec> arguments,
            @JsonProperty("derivedColumnName") String derivedColumnName,
            @JsonProperty("returnType") String returnType)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.functionName = functionName;
        this.parameterTypes = parameterTypes;
        this.arguments = arguments;
        this.derivedColumnName = derivedColumnName;
        this.returnType = returnType;
    }

    @JsonProperty
    public String getReturnType()
    {
        return returnType;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public List<String> getParameterTypes()
    {
        return parameterTypes;
    }

    @JsonProperty
    public List<DerivedColumnArgumentSpec> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public String getDerivedColumnName()
    {
        return derivedColumnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DerivedColumnUDFSpec that = (DerivedColumnUDFSpec) o;
        return Objects.equal(catalog, that.catalog)
                && Objects.equal(schema, that.schema)
                && Objects.equal(functionName, that.functionName)
                && Objects.equal(parameterTypes, that.parameterTypes)
                && Objects.equal(arguments, that.arguments)
                && Objects.equal(derivedColumnName, that.derivedColumnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(catalog, schema, functionName, parameterTypes, arguments, derivedColumnName);
    }
}
