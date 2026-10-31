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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * The proper columns of the table function are not known at function declaration time.
 * They must be determined at query analysis time based on the actual call arguments.
 */
public class GenericTableReturnTypeSpecification
        extends ReturnTypeSpecification
{
    public static final GenericTableReturnTypeSpecification GENERIC_TABLE = new GenericTableReturnTypeSpecification("");
    private static final String returnType = "GENERIC";

    @JsonCreator
    public GenericTableReturnTypeSpecification(@JsonProperty("returnType") String returnType)
    {}

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericTableReturnTypeSpecification that = (GenericTableReturnTypeSpecification) o;
        return Objects.equals(returnType, that.returnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(returnType);
    }

    @Override
    public String getReturnType()
    {
        return returnType;
    }
}
