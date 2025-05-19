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
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.metadata.Split;

import javax.inject.Inject;

import static com.facebook.presto.server.thrift.CustomCodecUtils.createSyntheticMetadata;
import static com.facebook.presto.server.thrift.CustomCodecUtils.readSingleJsonField;
import static com.facebook.presto.server.thrift.CustomCodecUtils.writeSingleJsonField;
import static java.util.Objects.requireNonNull;

public class SplitCodec
        implements ThriftCodec<Split>
{
    private static final short SPLIT_DATA_FIELD_ID = 1;
    private static final String SPLIT_DATA_FIELD_NAME = "split";
    private static final String SPLIT_DATA_STRUCT_NAME = "Split";

    private final ThriftCatalog thriftCatalog;
    private final ThriftType syntheticStructType;
    private final JsonCodec<Split> jsonCodec;

    @Inject
    public SplitCodec(JsonCodec<Split> jsonCodec, ThriftCatalog thriftCatalog)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.thriftCatalog = requireNonNull(thriftCatalog, "thriftCatalog is null");

        ThriftStructMetadata structMetadata = createSyntheticMetadata(thriftCatalog, SPLIT_DATA_FIELD_ID, SPLIT_DATA_FIELD_NAME, Split.class, String.class);
        this.syntheticStructType = ThriftType.struct(structMetadata);

        thriftCatalog.addThriftType(syntheticStructType);
    }

    public ThriftType getType()
    {
        return syntheticStructType;
    }

    @Override
    public Split read(TProtocolReader protocol)
            throws Exception
    {
        return readSingleJsonField(protocol, jsonCodec, SPLIT_DATA_FIELD_ID, SPLIT_DATA_FIELD_NAME);
    }

    @Override
    public void write(Split value, TProtocolWriter protocol)
            throws Exception
    {
        writeSingleJsonField(value, protocol, jsonCodec, SPLIT_DATA_FIELD_ID, SPLIT_DATA_FIELD_NAME, SPLIT_DATA_STRUCT_NAME);
    }
}
