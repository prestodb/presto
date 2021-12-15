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
package com.facebook.presto.functionNamespace.execution.grpc;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.grpc.udf.GrpcUdfPageFormat;

import static com.facebook.presto.grpc.udf.GrpcUdfPageFormat.Presto;

public class GrpcSqlFunctionExecutionConfig
{
    private String grpcAddress;
    private GrpcUdfPageFormat grpcUdfPageFormat = Presto;

    @Config("grpc.udf.address")
    public GrpcSqlFunctionExecutionConfig setGrpcAddress(String address)
    {
        this.grpcAddress = address;
        return this;
    }

    public String getGrpcAddress()
    {
        return grpcAddress;
    }

    @Config("grpc.udf.page-format")
    public GrpcSqlFunctionExecutionConfig setGrpcUdfPageFormat(String pageFormat)
    {
        this.grpcUdfPageFormat = GrpcUdfPageFormat.valueOf(pageFormat);
        return this;
    }

    public GrpcUdfPageFormat getGrpcUdfPageFormat()
    {
        return grpcUdfPageFormat;
    }
}
