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
package com.facebook.presto.jdbc;

import com.facebook.presto.spi.type.TypeSignature;

import static com.facebook.presto.jdbc.TypeInfo.Nullable;
import static java.util.Objects.requireNonNull;

public class ParameterInfo
{
    private final TypeInfo typeInfo;
    private final int position;

    public ParameterInfo(TypeInfo typeInfo, int position)
    {
        this.typeInfo = requireNonNull(typeInfo, "typeInfo is null");
        this.position = position;
    }

    public TypeSignature getParameterTypeSignature()
    {
        return typeInfo.getTypeSignature();
    }

    public String getParameterTypeName()
    {
        return typeInfo.getTypeSignature().toString();
    }

    public int getParameterType()
    {
        return typeInfo.getSqlType();
    }

    public Nullable getNullable()
    {
        return typeInfo.getNullable();
    }

    public boolean isSigned()
    {
        return typeInfo.isSigned();
    }

    public int getPrecision()
    {
        return typeInfo.getPrecision();
    }

    public int getScale()
    {
        return typeInfo.getScale();
    }

    public int getPosition()
    {
        return position;
    }

    static class Builder
    {
        private TypeInfo.Builder typeInfoBuilder = new TypeInfo.Builder();
        private int position;

        public Builder setTypeSignature(TypeSignature signature)
        {
            this.typeInfoBuilder.setTypeSignature(signature);
            return this;
        }

        public Builder setNullable(Nullable nullable)
        {
            this.typeInfoBuilder.setNullable(nullable);
            return this;
        }

        public Builder setCurrency(boolean currency)
        {
            this.typeInfoBuilder.setCurrency(currency);
            return this;
        }

        public Builder setPosition(int position)
        {
            this.position = position;
            return this;
        }

        public ParameterInfo build()
        {
            return new ParameterInfo(
                    typeInfoBuilder.build(),
                    position);
        }
    }
}
