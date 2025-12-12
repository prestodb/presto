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

public class DescriptorArgumentSpecification
        extends ArgumentSpecification
{
    public static final String argumentType = "DescriptorArgumentSpecification";
    @JsonCreator
    public DescriptorArgumentSpecification(
            @JsonProperty("name") String name,
            @JsonProperty("required") boolean required,
            @JsonProperty("defaultValue") Descriptor defaultValue)
    {
        super(name, required, defaultValue);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;
        private boolean required = true;
        private Descriptor defaultValue;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder defaultValue(Descriptor defaultValue)
        {
            this.required = false;
            this.defaultValue = defaultValue;
            return this;
        }

        public DescriptorArgumentSpecification build()
        {
            return new DescriptorArgumentSpecification(name, required, defaultValue);
        }
    }

    @Override
    public String getArgumentType()
    {
        return argumentType;
    }
}
