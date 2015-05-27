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

package com.facebook.presto.plugin.nullconnector;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class NullColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final int ordinalPosition;
    private final TypeSignature typeSignature;

    public NullColumnHandle(ColumnMetadata columnMetadata)
    {
        name = columnMetadata.getName();
        ordinalPosition = columnMetadata.getOrdinalPosition();
        typeSignature = columnMetadata.getType().getTypeSignature();
    }

    /*
     * Constructor used during serialization
     */
    @JsonCreator
    public NullColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("typeSignature") TypeSignature typeSignature)
    {
        this.name = name;
        this.ordinalPosition = ordinalPosition;
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    public ColumnMetadata toColumnMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(name, typeManager.getType(typeSignature), ordinalPosition, false);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, ordinalPosition, typeSignature);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NullColumnHandle other = (NullColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.typeSignature, other.typeSignature);
    }
}
