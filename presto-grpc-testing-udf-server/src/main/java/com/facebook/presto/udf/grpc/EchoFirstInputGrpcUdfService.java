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
package com.facebook.presto.udf.grpc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.grpc.udf.GrpcSerializedPage;
import com.facebook.presto.grpc.udf.GrpcUdfInvokeGrpc;
import com.facebook.presto.grpc.udf.GrpcUdfPage;
import com.facebook.presto.grpc.udf.GrpcUdfRequest;
import com.facebook.presto.grpc.udf.GrpcUdfResult;
import com.facebook.presto.grpc.udf.GrpcUdfStats;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;

import static com.facebook.presto.grpc.api.udf.GrpcUtils.toGrpcSerializedPage;
import static com.facebook.presto.grpc.api.udf.GrpcUtils.toGrpcUdfPage;
import static com.facebook.presto.grpc.api.udf.GrpcUtils.toPrestoPage;
import static com.facebook.presto.grpc.udf.GrpcUdfPageFormat.Presto;

public class EchoFirstInputGrpcUdfService
        extends GrpcUdfInvokeGrpc.GrpcUdfInvokeImplBase
{
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public EchoFirstInputGrpcUdfService(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = blockEncodingSerde;
    }

    @Override
    public void invokeUdf(GrpcUdfRequest request, StreamObserver<GrpcUdfResult> responseObserver)
    {
        GrpcUdfPage grpcUdfPage = request.getInputs();
        GrpcUdfPage result;
        switch (grpcUdfPage.getGrpcUdfPageFormat()) {
            case Presto:
                Page prestoPage = toPrestoPage(blockEncodingSerde, grpcUdfPage.getGrpcSerializedPage());
                if (prestoPage.getPositionCount() == 0) {
                    new UnsupportedOperationException("No input to echo");
                }
                GrpcSerializedPage grpcSerializedPage = toGrpcSerializedPage(blockEncodingSerde, new Page(prestoPage.getBlock(0)));
                result = toGrpcUdfPage(Presto, grpcSerializedPage);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        GrpcUdfResult grpcUdfResult = GrpcUdfResult.newBuilder()
                .setResult(result)
                .setUdfStats(GrpcUdfStats.newBuilder().setTotalCpuTimeMs(100).build())
                .build();

        responseObserver.onNext(grpcUdfResult);
        responseObserver.onCompleted();
    }
}
