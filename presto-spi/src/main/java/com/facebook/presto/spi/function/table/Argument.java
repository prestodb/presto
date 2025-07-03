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
 * This class represents the three types of arguments passed to a Table Function:
 * scalar arguments, descriptor arguments, and table arguments.
 * <p>
 * This representation should be considered experimental. Eventually, {@link ConnectorExpression}
 * should be extended to include the Table Function arguments.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DescriptorArgument.class, name = "descriptor"),
        @JsonSubTypes.Type(value = ScalarArgument.class, name = "scalar"),
        @JsonSubTypes.Type(value = TableArgument.class, name = "table"),
})
public abstract class Argument
{
}
