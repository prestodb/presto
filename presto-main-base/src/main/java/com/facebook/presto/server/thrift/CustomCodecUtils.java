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
package com.facebook.presto.server.thrift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.TException;
import com.facebook.drift.codec.metadata.DefaultThriftTypeReference;
import com.facebook.drift.codec.metadata.FieldKind;
import com.facebook.drift.codec.metadata.ThriftFieldExtractor;
import com.facebook.drift.codec.metadata.ThriftFieldMetadata;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.drift.annotations.ThriftField.Requiredness.NONE;
import static java.lang.String.format;

/***
 * When we need a custom codec for a primitive type, we need a wrapper to pass the needsCodec check within ThriftCodecByteCodeGenerator.java
 */
public class CustomCodecUtils
{
    private CustomCodecUtils() {}

    public static ThriftStructMetadata createSyntheticMetadata(short fieldId, String fieldName, Class<?> originalType, Class<?> referencedType, ThriftType thriftType)
    {
        ThriftFieldMetadata fieldMetaData = new ThriftFieldMetadata(
                fieldId,
                false, false, NONE, ImmutableMap.of(),
                new DefaultThriftTypeReference(thriftType),
                fieldName,
                FieldKind.THRIFT_FIELD,
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new ThriftFieldExtractor(
                        fieldId,
                        fieldName,
                        FieldKind.THRIFT_FIELD,
                        originalType.getDeclaredFields()[0], // Any field should work since we are handing extraction in codec on our own
                        referencedType)),
                Optional.empty());
        return new ThriftStructMetadata(
                originalType.getSimpleName() + "Wrapper",
                ImmutableMap.of(),
                originalType, null,
                ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(), ImmutableList.of(), ImmutableList.of(fieldMetaData), Optional.empty(), ImmutableList.of());
    }

    public static <T> T readSingleJsonField(TProtocolReader protocol, JsonCodec<T> jsonCodec, short fieldId, String fieldName)
            throws TException
    {
        protocol.readStructBegin();
        String jsonValue = null;
        TField field = protocol.readFieldBegin();
        while (field.getType() != TType.STOP) {
            if (field.getId() == fieldId) {
                if (field.getType() == TType.STRING) {
                    jsonValue = protocol.readString();
                }
                else {
                    throw new TProtocolException(format("Unexpected field type: %s for field %s", field.getType(), fieldName));
                }
            }
            protocol.readFieldEnd();
            field = protocol.readFieldBegin();
        }
        protocol.readStructEnd();

        if (jsonValue == null) {
            throw new TProtocolException(format("Required field '%s' was not found", fieldName));
        }
        return jsonCodec.fromJson(jsonValue);
    }

    public static <T> void writeSingleJsonField(T value, TProtocolWriter protocol, JsonCodec<T> jsonCodec, short fieldId, String fieldName, String structName)
            throws TException
    {
        protocol.writeStructBegin(new TStruct(structName));

        protocol.writeFieldBegin(new TField(fieldName, TType.STRING, fieldId));
        protocol.writeString(jsonCodec.toJson(value));
        protocol.writeFieldEnd();

        protocol.writeFieldStop();
        protocol.writeStructEnd();
    }
}
