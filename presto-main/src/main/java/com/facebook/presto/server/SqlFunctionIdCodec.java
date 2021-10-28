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

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.spi.function.SqlFunctionId;

// TODO: convert SqlFunctionId to be a native Thrift struct
public class SqlFunctionIdCodec
        implements ThriftCodec<SqlFunctionId>
{
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.STRING, SqlFunctionId.class);
    }

    @Override
    public SqlFunctionId read(TProtocolReader protocol)
            throws Exception
    {
        return SqlFunctionId.parseSqlFunctionId(protocol.readString());
    }

    @Override
    public void write(SqlFunctionId value, TProtocolWriter protocol)
            throws Exception
    {
        protocol.writeString(value.toJsonString());
    }
}
