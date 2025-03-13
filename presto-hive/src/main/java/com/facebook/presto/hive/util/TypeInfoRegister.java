package com.facebook.presto.hive.util;

import alluxio.exception.status.UnimplementedException;
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
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TTransportException;

import static java.lang.String.format;

public class TypeInfoRegister
{
    static {
        ThriftSerializationRegistry.registerSerializer(PrimitiveTypeInfo.class, TypeInfoRegister::toThriftPrimitiveTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(PrimitiveTypeInfo.class, ThriftPrimitiveTypeInfo.class, TypeInfoRegister::deserializePrimitiveTypeInfo, TypeInfoRegister::fromThriftPrimitiveTypeInfo);

        ThriftSerializationRegistry.registerSerializer(VarcharTypeInfo.class, TypeInfoRegister::toThriftVarcharTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(VarcharTypeInfo.class, ThriftVarcharTypeInfo.class, TypeInfoRegister::deserializeVarcharTypeInfo, TypeInfoRegister::fromThriftVarcharTypeInfo);

        ThriftSerializationRegistry.registerSerializer(CharTypeInfo.class, TypeInfoRegister::toThriftCharTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(CharTypeInfo.class, ThriftCharTypeInfo.class, TypeInfoRegister::deserializeCharTypeInfo, TypeInfoRegister::fromThriftCharTypeInfo);

        ThriftSerializationRegistry.registerSerializer(DecimalTypeInfo.class, TypeInfoRegister::toThriftDecimalTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(DecimalTypeInfo.class, ThriftDecimalTypeInfo.class, TypeInfoRegister::deserializeDecimalTypeInfo, TypeInfoRegister::fromThriftDecimalTypeInfo);
    }

    public static ThriftTypeInfo toThriftInterface(TypeInfo typeInfo)
    {
        throw new RuntimeException("ThriftTypeInfo toThriftInterface not implemented");
    }

    public static ThriftTypeInfo toThrift(TypeInfo typeInfo)
            throws UnimplementedException
    {
        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            ThriftTypeInfo thriftTypeInfo = new ThriftTypeInfo();
            thriftTypeInfo.setType(typeInfo.getClass().getName());

            if (typeInfo instanceof DecimalTypeInfo) {
                thriftTypeInfo.setSerializedTypeInfo(serializer.serialize(toThriftDecimalTypeInfo((DecimalTypeInfo) typeInfo)));
            }
            else if (typeInfo instanceof VarcharTypeInfo) {
                thriftTypeInfo.setSerializedTypeInfo(serializer.serialize(toThriftVarcharTypeInfo((VarcharTypeInfo) typeInfo)));
            }
            else if (typeInfo instanceof CharTypeInfo) {
                thriftTypeInfo.setSerializedTypeInfo(serializer.serialize(toThriftCharTypeInfo((CharTypeInfo) typeInfo)));
            }
            else if (typeInfo instanceof PrimitiveTypeInfo) {
                thriftTypeInfo.setSerializedTypeInfo(serializer.serialize(toThriftPrimitiveTypeInfo((PrimitiveTypeInfo) typeInfo)));
            }
            else {
                throw new UnimplementedException(format("type info register not implemented: %s", typeInfo.getClass().getName()));
            }

            return thriftTypeInfo;
        }
        catch (TTransportException e) {
            throw new RuntimeException(e);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static ThriftPrimitiveTypeInfo toThriftPrimitiveTypeInfo(PrimitiveTypeInfo typeInfo)
    {
        return new ThriftPrimitiveTypeInfo(typeInfo.getTypeName());
    }

    public static PrimitiveTypeInfo deserializePrimitiveTypeInfo(byte[] bytes)
    {
        try {
            ThriftPrimitiveTypeInfo thriftType = new ThriftPrimitiveTypeInfo();
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            deserializer.deserialize(thriftType, bytes);
            return fromThriftPrimitiveTypeInfo(thriftType);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
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
        return new ThriftVarcharTypeInfo();
    }

    public static ThriftCharTypeInfo toThriftCharTypeInfo(CharTypeInfo typeInfo)
    {
        if (typeInfo.getLength() != 0) {
            return new ThriftCharTypeInfo(typeInfo.getLength());
        }
        return new ThriftCharTypeInfo();
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
        try {
            ThriftVarcharTypeInfo thriftType = new ThriftVarcharTypeInfo();
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            deserializer.deserialize(thriftType, bytes);
            return fromThriftVarcharTypeInfo(thriftType);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static CharTypeInfo deserializeCharTypeInfo(byte[] bytes)
    {
        try {
            ThriftCharTypeInfo thriftType = new ThriftCharTypeInfo();
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            deserializer.deserialize(thriftType, bytes);
            return fromThriftCharTypeInfo(thriftType);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static DecimalTypeInfo deserializeDecimalTypeInfo(byte[] bytes)
    {
        try {
            ThriftDecimalTypeInfo thriftType = new ThriftDecimalTypeInfo();
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            deserializer.deserialize(thriftType, bytes);
            return fromThriftDecimalTypeInfo(thriftType);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
