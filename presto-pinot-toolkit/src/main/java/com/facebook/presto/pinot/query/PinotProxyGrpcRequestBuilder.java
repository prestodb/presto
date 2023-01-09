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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotErrorCode;
import com.facebook.presto.pinot.PinotException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.spi.utils.CommonConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PinotProxyGrpcRequestBuilder
        extends GrpcRequestBuilder
{
    private static final String KEY_OF_PROXY_GRPC_FORWARD_HOST = "FORWARD_HOST";
    private static final String KEY_OF_PROXY_GRPC_FORWARD_PORT = "FORWARD_PORT";

    private String hostName;
    private int port = -1;
    private int requestId;
    private String brokerId = "unknown";
    private boolean enableTrace;
    private boolean enableStreaming;
    private String payloadType;
    private String sql;
    private List<String> segments;
    private Map<String, String> extraMetadata = new HashMap<>();

    public PinotProxyGrpcRequestBuilder setHostName(String hostName)
    {
        this.hostName = hostName;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setPort(int port)
    {
        this.port = port;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setRequestId(int requestId)
    {
        this.requestId = requestId;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setBrokerId(String brokerId)
    {
        this.brokerId = brokerId;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setEnableTrace(boolean enableTrace)
    {
        this.enableTrace = enableTrace;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setEnableStreaming(boolean enableStreaming)
    {
        this.enableStreaming = enableStreaming;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setSql(String sql)
    {
        payloadType = CommonConstants.Query.Request.PayloadType.SQL;
        this.sql = sql;
        return this;
    }

    public PinotProxyGrpcRequestBuilder addExtraMetadata(Map<String, String> extraMetadata)
    {
        this.extraMetadata.putAll(extraMetadata);
        return this;
    }

    public PinotProxyGrpcRequestBuilder setSegments(List<String> segments)
    {
        this.segments = segments;
        return this;
    }

    public Server.ServerRequest build()
    {
        if (payloadType == null || segments.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_INVALID_SEGMENT_QUERY_GENERATED, Optional.empty(), "Query and segmentsToQuery must be set");
        }
        if (!payloadType.equals(CommonConstants.Query.Request.PayloadType.SQL)) {
            throw new RuntimeException("Only [SQL] Payload type is allowed: " + payloadType);
        }
        Map<String, String> metadata = new HashMap<>();
        metadata.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Integer.toString(requestId));
        metadata.put(CommonConstants.Query.Request.MetadataKeys.BROKER_ID, brokerId);
        metadata.put(CommonConstants.Query.Request.MetadataKeys.ENABLE_TRACE, Boolean.toString(enableTrace));
        metadata.put(CommonConstants.Query.Request.MetadataKeys.ENABLE_STREAMING, Boolean.toString(enableStreaming));
        metadata.put(CommonConstants.Query.Request.MetadataKeys.PAYLOAD_TYPE, payloadType);
        if (this.hostName != null) {
            metadata.put(KEY_OF_PROXY_GRPC_FORWARD_HOST, this.hostName);
        }
        if (this.port > 0) {
            metadata.put(KEY_OF_PROXY_GRPC_FORWARD_PORT, String.valueOf(this.port));
        }
        extraMetadata.forEach((k, v) -> metadata.put(k, v));
        return Server.ServerRequest.newBuilder()
            .putAllMetadata(metadata)
            .setSql(sql)
            .addAllSegments(segments)
            .build();
    }
}
