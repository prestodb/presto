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
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.execution.scheduler.TableWriteInfo;

import javax.inject.Inject;

import static com.facebook.presto.server.thrift.CustomCodecUtils.createSyntheticMetadata;
import static com.facebook.presto.server.thrift.CustomCodecUtils.readSingleJsonField;
import static com.facebook.presto.server.thrift.CustomCodecUtils.writeSingleJsonField;
import static java.util.Objects.requireNonNull;

public class TableWriteInfoCodec
        implements ThriftCodec<TableWriteInfo>
{
    private static final short TABLE_WRITE_INFO_DATA_FIELD_ID = 1;
    private static final String TABLE_WRITE_INFO_DATA_FIELD_NAME = "tableWriteInfo";
    private static final String TABLE_WRITE_INFO_STRUCT_NAME = "TableWriteInfo";
    private static final ThriftType SYNTHETIC_STRUCT_TYPE = ThriftType.struct(createSyntheticMetadata(TABLE_WRITE_INFO_DATA_FIELD_ID, TABLE_WRITE_INFO_DATA_FIELD_NAME, TableWriteInfo.class, String.class, ThriftType.STRING));

    private final JsonCodec<TableWriteInfo> jsonCodec;

    @Inject
    public TableWriteInfoCodec(JsonCodec<TableWriteInfo> jsonCodec, ThriftCatalog thriftCatalog)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        thriftCatalog.addThriftType(SYNTHETIC_STRUCT_TYPE);
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return SYNTHETIC_STRUCT_TYPE;
    }

    @Override
    public ThriftType getType()
    {
        return SYNTHETIC_STRUCT_TYPE;
    }

    @Override
    public TableWriteInfo read(TProtocolReader protocol)
            throws Exception
    {
        return readSingleJsonField(protocol, jsonCodec, TABLE_WRITE_INFO_DATA_FIELD_ID, TABLE_WRITE_INFO_DATA_FIELD_NAME);
    }

    @Override
    public void write(TableWriteInfo value, TProtocolWriter protocol)
            throws Exception
    {
        writeSingleJsonField(value, protocol, jsonCodec, TABLE_WRITE_INFO_DATA_FIELD_ID, TABLE_WRITE_INFO_DATA_FIELD_NAME, TABLE_WRITE_INFO_STRUCT_NAME);
    }
}
