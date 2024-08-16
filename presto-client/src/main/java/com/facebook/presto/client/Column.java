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
package com.facebook.presto.client;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
@ThriftStruct
public class Column
{
    private final String name;
    private final String type;
    private final ClientTypeSignature typeSignature;

    public Column(String name, Type type)
    {
        this(name, type.getTypeSignature());
    }

    public Column(String name, TypeSignature signature)
    {
        this(name, signature.toString(), new ClientTypeSignature(signature));
    }

    @JsonCreator
    @ThriftConstructor
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("typeSignature") ClientTypeSignature typeSignature)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @JsonProperty
    @ThriftField(2)
    public String getType()
    {
        return type;
    }

    @JsonProperty
    @ThriftField(3)
    public ClientTypeSignature getTypeSignature()
    {
        return typeSignature;
    }
}
