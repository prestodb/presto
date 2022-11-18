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
package com.facebook.presto.sql.analyzer.crux;

import static java.util.Objects.requireNonNull;

/**
 * TODO: The types are hardcoded and hacky, these should be cleaned up.
 */
public class Type
{
    public static final PrimitiveType INT8 = new PrimitiveType(PrimitiveTypeKind.INT8);
    public static final PrimitiveType INT16 = new PrimitiveType(PrimitiveTypeKind.INT16);
    public static final PrimitiveType INT32 = new PrimitiveType(PrimitiveTypeKind.INT32);
    public static final PrimitiveType INT64 = new PrimitiveType(PrimitiveTypeKind.INT64);
    public static final PrimitiveType STRING = new PrimitiveType(PrimitiveTypeKind.STRING);
    public static final PrimitiveType BOOL = new PrimitiveType(PrimitiveTypeKind.BOOL);
    public static final PrimitiveType FLOAT32 = new PrimitiveType(PrimitiveTypeKind.FLOAT32);
    public static final PrimitiveType FLOAT64 = new PrimitiveType(PrimitiveTypeKind.FLOAT64);
    private final TypeKind typeKind;

    protected Type(TypeKind typeKind)
    {
        this.typeKind = requireNonNull(typeKind, "typeKind is null");
    }

    public TypeKind getTypeKind()
    {
        return typeKind;
    }

    public boolean isCallable()
    {
        return this instanceof CallableType;
    }

    public CallableType asCallable()
    {
        return (CallableType) this;
    }

    public boolean isPrimitive()
    {
        return this instanceof PrimitiveType;
    }

    public PrimitiveType asPrimitive()
    {
        return (PrimitiveType) this;
    }
}
