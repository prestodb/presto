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
package com.facebook.presto.common.type;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftUnion;
import com.facebook.drift.annotations.ThriftUnionId;
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;

import java.util.Objects;

import static com.facebook.presto.common.type.BigintEnumType.LongEnumMap;
import static com.facebook.presto.common.type.ParameterKind.DISTINCT_TYPE;
import static com.facebook.presto.common.type.ParameterKind.LONG;
import static com.facebook.presto.common.type.ParameterKind.LONG_ENUM;
import static com.facebook.presto.common.type.ParameterKind.NAMED_TYPE;
import static com.facebook.presto.common.type.ParameterKind.TYPE;
import static com.facebook.presto.common.type.ParameterKind.VARCHAR_ENUM;
import static com.facebook.presto.common.type.ParameterKind.VARIABLE;

@ThriftUnion
public class TypeSignatureParameterUnion
{
    private TypeSignature typeSignature;
    private Long longLiteral;
    private NamedTypeSignature namedTypeSignature;
    private String variable;
    private BigintEnumType.LongEnumMap longEnumMap;
    private VarcharEnumMap varcharEnumMap;
    private DistinctTypeInfo distinctTypeInfo;
    private final short id;

    @ThriftConstructor
    public TypeSignatureParameterUnion(TypeSignature typeSignature)
    {
        this.typeSignature = typeSignature;
        this.id = (short) TYPE.getValue();
    }

    @ThriftField(1)
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    @ThriftConstructor
    public TypeSignatureParameterUnion(NamedTypeSignature namedTypeSignature)
    {
        this.namedTypeSignature = namedTypeSignature;
        this.id = (short) NAMED_TYPE.getValue();
    }

    @ThriftField(2)
    public NamedTypeSignature getNamedTypeSignature()
    {
        return namedTypeSignature;
    }

    @ThriftConstructor
    public TypeSignatureParameterUnion(Long longLiteral)
    {
        this.longLiteral = longLiteral;
        this.id = (short) LONG.getValue();
    }

    @ThriftField(3)
    public Long getLongLiteral()
    {
        return longLiteral;
    }

    @ThriftConstructor
    public TypeSignatureParameterUnion(String variable)
    {
        this.variable = variable;
        this.id = (short) VARIABLE.getValue();
    }

    @ThriftField(4)
    public String getVariable()
    {
        return variable;
    }

    @ThriftConstructor
    public TypeSignatureParameterUnion(LongEnumMap longEnumMap)
    {
        this.longEnumMap = longEnumMap;
        this.id = (short) LONG_ENUM.getValue();
    }

    @ThriftField(5)
    public LongEnumMap getLongEnumMap()
    {
        return longEnumMap;
    }

    @ThriftConstructor
    public TypeSignatureParameterUnion(VarcharEnumMap varcharEnumMap)
    {
        this.varcharEnumMap = varcharEnumMap;
        this.id = (short) VARCHAR_ENUM.getValue();
    }

    @ThriftField(6)
    public VarcharEnumMap getVarcharEnumMap()
    {
        return varcharEnumMap;
    }

    @ThriftConstructor
    public TypeSignatureParameterUnion(DistinctTypeInfo distinctTypeInfo)
    {
        this.distinctTypeInfo = distinctTypeInfo;
        this.id = (short) DISTINCT_TYPE.getValue();
    }

    @ThriftField(7)
    public DistinctTypeInfo getDistinctTypeInfo()
    {
        return distinctTypeInfo;
    }

    @ThriftUnionId
    public short getId()
    {
        return id;
    }

    public static TypeSignatureParameterUnion convertToTypeSignatureParameterUnion(Object value)
    {
        if (value instanceof TypeSignature) {
            return new TypeSignatureParameterUnion((TypeSignature) value);
        }
        else if (value instanceof Long) {
            return new TypeSignatureParameterUnion((Long) value);
        }
        else if (value instanceof NamedTypeSignature) {
            return new TypeSignatureParameterUnion((NamedTypeSignature) value);
        }
        else if (value instanceof String) {
            return new TypeSignatureParameterUnion((String) value);
        }
        else if (value instanceof LongEnumMap) {
            return new TypeSignatureParameterUnion((LongEnumMap) value);
        }
        else if (value instanceof VarcharEnumMap) {
            return new TypeSignatureParameterUnion((VarcharEnumMap) value);
        }
        else if (value instanceof DistinctTypeInfo) {
            return new TypeSignatureParameterUnion((DistinctTypeInfo) value);
        }
        else {
            throw new IllegalArgumentException("value is of an unknown type: " + value.getClass().getName());
        }
    }

    public static Object convertToValue(TypeSignatureParameterUnion parameterUnion)
    {
        if (parameterUnion.getTypeSignature() != null) {
            return parameterUnion.getTypeSignature();
        }
        else if (parameterUnion.getLongLiteral() != null) {
            return parameterUnion.getLongLiteral();
        }
        else if (parameterUnion.getNamedTypeSignature() != null) {
            return parameterUnion.getNamedTypeSignature();
        }
        else if (parameterUnion.getVariable() != null) {
            return parameterUnion.getVariable();
        }
        else if (parameterUnion.getLongEnumMap() != null) {
            return parameterUnion.getLongEnumMap();
        }
        else if (parameterUnion.getVarcharEnumMap() != null) {
            return parameterUnion.getVarcharEnumMap();
        }
        else if (parameterUnion.getDistinctTypeInfo() != null) {
            return parameterUnion.getDistinctTypeInfo();
        }
        else {
            throw new IllegalArgumentException("TypeSignatureParameterUnion is of an unknown type: " + parameterUnion.getClass().getName());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypeSignatureParameterUnion other = (TypeSignatureParameterUnion) o;

        return Objects.equals(this.typeSignature, other.typeSignature) &&
                Objects.equals(this.longLiteral, other.longLiteral) &&
                Objects.equals(this.namedTypeSignature, other.namedTypeSignature) &&
                Objects.equals(this.variable, other.variable) &&
                Objects.equals(this.longEnumMap, other.longEnumMap) &&
                Objects.equals(this.varcharEnumMap, other.varcharEnumMap) &&
                Objects.equals(this.distinctTypeInfo, other.distinctTypeInfo) &&
                Objects.equals(this.id, other.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeSignature, longLiteral, namedTypeSignature, variable, longEnumMap, varcharEnumMap, distinctTypeInfo, id);
    }
}
