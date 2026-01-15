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

/**
 * The table function has no proper columns.
 */
public class OnlyPassThroughReturnTypeSpecification
        extends ReturnTypeSpecification
{
    public static final OnlyPassThroughReturnTypeSpecification ONLY_PASS_THROUGH = new OnlyPassThroughReturnTypeSpecification("");
    private static final String returnType = "PASSTRHOUGH";

    @JsonCreator
    public OnlyPassThroughReturnTypeSpecification(@JsonProperty("returnType") String returnType)
    {
    }

    @Override
    public String getReturnType()
    {
        return returnType;
    }
}
