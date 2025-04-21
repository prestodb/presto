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
package com.facebook.presto.hive.util;

import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftCharTypeInfo;
import com.facebook.presto.common.experimental.auto_gen.ThriftDecimalTypeInfo;
import com.facebook.presto.common.experimental.auto_gen.ThriftPrimitiveTypeInfo;
import com.facebook.presto.common.experimental.auto_gen.ThriftTypeInfo;
import com.facebook.presto.common.experimental.auto_gen.ThriftVarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import static java.lang.String.format;

public class HiveTypeInfoRegister
{
    private HiveTypeInfoRegister() {}

    public static void registerHiveTypeInfo()
    {
        ThriftSerializationRegistry.registerSerializer(PrimitiveTypeInfo.class, HiveTypeInfoRegister::toThriftPrimitiveTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(PrimitiveTypeInfo.class, ThriftPrimitiveTypeInfo.class, HiveTypeInfoRegister::deserializePrimitiveTypeInfo, HiveTypeInfoRegister::fromThriftPrimitiveTypeInfo);

        ThriftSerializationRegistry.registerSerializer(VarcharTypeInfo.class, HiveTypeInfoRegister::toThriftVarcharTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(VarcharTypeInfo.class, ThriftVarcharTypeInfo.class, HiveTypeInfoRegister::deserializeVarcharTypeInfo, HiveTypeInfoRegister::fromThriftVarcharTypeInfo);

        ThriftSerializationRegistry.registerSerializer(CharTypeInfo.class, HiveTypeInfoRegister::toThriftCharTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(CharTypeInfo.class, ThriftCharTypeInfo.class, HiveTypeInfoRegister::deserializeCharTypeInfo, HiveTypeInfoRegister::fromThriftCharTypeInfo);

        ThriftSerializationRegistry.registerSerializer(DecimalTypeInfo.class, HiveTypeInfoRegister::toThriftDecimalTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(DecimalTypeInfo.class, ThriftDecimalTypeInfo.class, HiveTypeInfoRegister::deserializeDecimalTypeInfo, HiveTypeInfoRegister::fromThriftDecimalTypeInfo);
    }

    public static ThriftTypeInfo toThriftInterface(TypeInfo typeInfo)
    {
        throw new RuntimeException("ThriftTypeInfo toThriftInterface not implemented");
    }

    public static ThriftTypeInfo toThrift(TypeInfo typeInfo)
    {
        ThriftTypeInfo.Builder builder = ThriftTypeInfo.builder();
        builder.setType(typeInfo.getClass().getName());

        if (typeInfo instanceof DecimalTypeInfo) {
            builder.setSerializedTypeInfo(FbThriftUtils.serialize(toThriftDecimalTypeInfo((DecimalTypeInfo) typeInfo)));
        }
        else if (typeInfo instanceof VarcharTypeInfo) {
            builder.setSerializedTypeInfo(FbThriftUtils.serialize(toThriftVarcharTypeInfo((VarcharTypeInfo) typeInfo)));
        }
        else if (typeInfo instanceof CharTypeInfo) {
            builder.setSerializedTypeInfo(FbThriftUtils.serialize(toThriftCharTypeInfo((CharTypeInfo) typeInfo)));
        }
        else if (typeInfo instanceof PrimitiveTypeInfo) {
            builder.setSerializedTypeInfo(FbThriftUtils.serialize(toThriftPrimitiveTypeInfo((PrimitiveTypeInfo) typeInfo)));
        }
        else {
            throw new RuntimeException(format("type info register not implemented: %s", typeInfo.getClass().getName()));
        }

        return builder.build();
    }

    public static ThriftPrimitiveTypeInfo toThriftPrimitiveTypeInfo(PrimitiveTypeInfo typeInfo)
    {
        return new ThriftPrimitiveTypeInfo(typeInfo.getTypeName());
    }

    public static PrimitiveTypeInfo deserializePrimitiveTypeInfo(byte[] bytes)
    {
        return fromThriftPrimitiveTypeInfo(FbThriftUtils.deserialize(ThriftPrimitiveTypeInfo.class, bytes));
    }

    public static PrimitiveTypeInfo fromThriftPrimitiveTypeInfo(ThriftPrimitiveTypeInfo thriftTypeInfo)
    {
        PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
        typeInfo.setTypeName(thriftTypeInfo.getTypeName());
        return typeInfo;
    }

    public static ThriftVarcharTypeInfo toThriftVarcharTypeInfo(VarcharTypeInfo typeInfo)
    {
        if (typeInfo.getLength() != 0) {
            return new ThriftVarcharTypeInfo(typeInfo.getLength());
        }
        return ThriftVarcharTypeInfo.builder().build();
    }

    public static ThriftCharTypeInfo toThriftCharTypeInfo(CharTypeInfo typeInfo)
    {
        if (typeInfo.getLength() != 0) {
            return new ThriftCharTypeInfo(typeInfo.getLength());
        }
        return ThriftCharTypeInfo.builder().build();
    }

    public static Object fromThrift(ThriftTypeInfo thriftTypeInfo)
    {
        return ThriftSerializationRegistry.deserialize(
                thriftTypeInfo.getType(),
                thriftTypeInfo.getSerializedTypeInfo());
    }

    public static ThriftDecimalTypeInfo toThriftDecimalTypeInfo(DecimalTypeInfo typeInfo)
    {
        return new ThriftDecimalTypeInfo(typeInfo.getPrecision(), typeInfo.getScale());
    }

    public static VarcharTypeInfo fromThriftVarcharTypeInfo(ThriftVarcharTypeInfo thriftTypeInfo)
    {
        if (thriftTypeInfo.getLength() != 0) {
            return new VarcharTypeInfo(thriftTypeInfo.getLength());
        }
        return new VarcharTypeInfo();
    }

    public static CharTypeInfo fromThriftCharTypeInfo(ThriftCharTypeInfo thriftTypeInfo)
    {
        if (thriftTypeInfo.getLength() != 0) {
            return new CharTypeInfo(thriftTypeInfo.getLength());
        }
        return new CharTypeInfo();
    }

    public static DecimalTypeInfo fromThriftDecimalTypeInfo(ThriftDecimalTypeInfo thriftTypeInfo)
    {
        return new DecimalTypeInfo(thriftTypeInfo.getPrecision(), thriftTypeInfo.getScale());
    }

    public static VarcharTypeInfo deserializeVarcharTypeInfo(byte[] bytes)
    {
        return fromThriftVarcharTypeInfo(FbThriftUtils.deserialize(ThriftVarcharTypeInfo.class, bytes));
    }

    public static CharTypeInfo deserializeCharTypeInfo(byte[] bytes)
    {
        return fromThriftCharTypeInfo(FbThriftUtils.deserialize(ThriftCharTypeInfo.class, bytes));
    }

    public static DecimalTypeInfo deserializeDecimalTypeInfo(byte[] bytes)
    {
        return fromThriftDecimalTypeInfo(FbThriftUtils.deserialize(ThriftDecimalTypeInfo.class, bytes));
    }
}
