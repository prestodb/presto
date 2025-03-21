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
package com.facebook.presto.server;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.spi.function.SqlInvokedFunction;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

// TODO: convert SqlInvokedFunction to be a native Thrift struct
public class SqlInvokedFunctionCodec
        implements ThriftCodec<SqlInvokedFunction>
{
    private final JsonCodec<SqlInvokedFunction> jsonCodec;

    @Inject
    public SqlInvokedFunctionCodec(JsonCodec<SqlInvokedFunction> jsonCodec)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    public ThriftType getType()
    {
        return new ThriftType(ThriftType.STRING, SqlInvokedFunction.class);
    }

    @Override
    public SqlInvokedFunction read(TProtocolReader protocol)
            throws Exception
    {
        return jsonCodec.fromJson(protocol.readString());
    }

    @Override
    public void write(SqlInvokedFunction value, TProtocolWriter protocol)
            throws Exception
    {
        protocol.writeString(jsonCodec.toJson(value));
    }
}
