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
package com.facebook.presto.hive.thrift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.hive.HiveSplit;

import javax.inject.Inject;

import static com.facebook.presto.hive.thrift.ThriftCodecUtils.createSyntheticMetadata;
import static com.facebook.presto.hive.thrift.ThriftCodecUtils.readSingleJsonField;
import static com.facebook.presto.hive.thrift.ThriftCodecUtils.writeSingleJsonField;
import static java.util.Objects.requireNonNull;

public class BucketConversionThriftCodec
        implements ThriftCodec<HiveSplit.BucketConversion>
{
    private static final short JSON_DATA_FIELD_ID = 111;
    private static final String JSON_DATA_FIELD_NAME = "bucketConversion";
    private static final String JSON_DATA_STRUCT_NAME = "BucketConversion";
    private static final ThriftType SYNTHETIC_STRUCT_TYPE = ThriftType.struct(createSyntheticMetadata(JSON_DATA_FIELD_ID, JSON_DATA_FIELD_NAME, HiveSplit.BucketConversion.class, String.class, ThriftType.STRING));

    private final JsonCodec<HiveSplit.BucketConversion> jsonCodec;

    @Inject
    public BucketConversionThriftCodec(JsonCodec<HiveSplit.BucketConversion> jsonCodec)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
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
    public HiveSplit.BucketConversion read(TProtocolReader protocol)
            throws Exception
    {
        return readSingleJsonField(protocol, jsonCodec, JSON_DATA_FIELD_ID, JSON_DATA_FIELD_NAME);
    }

    @Override
    public void write(HiveSplit.BucketConversion value, TProtocolWriter protocol)
            throws Exception
    {
        writeSingleJsonField(value, protocol, jsonCodec, JSON_DATA_FIELD_ID, JSON_DATA_FIELD_NAME, JSON_DATA_STRUCT_NAME);
    }
}
