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
package com.facebook.presto.server.thrift.codec;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.execution.ExecutionFailureInfo;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

// Hack: Drift has spotty support for recursive data structures.  Use JSON to serialize.
public class ExecutionFailureInfoCodec
        implements ThriftCodec<ExecutionFailureInfo>
{
    private static final JsonCodec<ExecutionFailureInfo> codec = JsonCodec.jsonCodec(ExecutionFailureInfo.class);

    @Inject
    public ExecutionFailureInfoCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.STRING, ExecutionFailureInfo.class, null);
    }

    @Override
    public ExecutionFailureInfo read(TProtocolReader protocol)
            throws Exception
    {
        return stringToExecutionFailureInfo(protocol.readString());
    }

    @Override
    public void write(ExecutionFailureInfo executionFailureInfo, TProtocolWriter protocol)
            throws Exception
    {
        requireNonNull(executionFailureInfo, "executionFailureInfo is null");
        protocol.writeString(executionFailureInfoToString(executionFailureInfo));
    }

    @FromThrift
    public static ExecutionFailureInfo stringToExecutionFailureInfo(String string)
    {
        return codec.fromJson(string);
    }

    @ToThrift
    public static String executionFailureInfoToString(ExecutionFailureInfo executionFailureInfo)
    {
        return codec.toJson(executionFailureInfo);
    }
}
