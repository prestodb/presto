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

import com.facebook.airlift.log.Logger;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.DefaultThriftTypeReference;
import com.facebook.drift.codec.metadata.FieldKind;
import com.facebook.drift.codec.metadata.ThriftFieldMetadata;
import com.facebook.drift.codec.metadata.ThriftMethodInjection;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TupleDomainCodec
        implements ThriftCodec<TupleDomain<ColumnHandle>>
{
    private static final ThriftType THRIFT_TYPE;

    private static final Logger log = Logger.get(TupleDomainCodec.class);

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public ThriftType getType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public TupleDomain<ColumnHandle> read(TProtocolReader protocol) throws Exception
    {
        return null;
    }

    @Override
    public void write(TupleDomain<ColumnHandle> value, TProtocolWriter protocol) throws Exception
    {
        Optional<List<TupleDomain.ColumnDomain<ColumnHandle>>> values = value.getColumnDomains();
        values.get();
        value.isAll();
        value.isNone();
    }
    static {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    (short) 1,
                    false,
                    false,
                    ThriftField.Requiredness.REQUIRED,
                    ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    "signature",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(TupleDomain.class.getDeclaredMethod("getColumnDomains"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fields.add(new ThriftFieldMetadata(
                    (short) 2,
                    false,
                    false,
                    ThriftField.Requiredness.REQUIRED,
                    ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BOOL),
                    "domains",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(TupleDomain.class.getDeclaredMethod("getDomains"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (Exception e) {
            throw new RuntimeException("Error building ThriftFieldMetadata for TypeCodec", e);
        }

        THRIFT_TYPE = ThriftType.struct(new ThriftStructMetadata(
                "TupleDomain",
                ImmutableMap.of(),
                TupleDomain.class,
                null,
                ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(),
                ImmutableList.of(),
                fields,
                Optional.empty(),
                ImmutableList.of()));
    }
}
