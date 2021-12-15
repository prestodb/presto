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
package com.facebook.presto.grpc.api.udf;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.grpc.udf.GrpcSerializedPage;
import com.facebook.presto.grpc.udf.GrpcUdfPage;
import com.facebook.presto.grpc.udf.GrpcUdfPageFormat;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.protobuf.ByteString;
import io.airlift.slice.Slices;

import java.util.Optional;

public class GrpcUtils
{
    private GrpcUtils() {}

    public static GrpcSerializedPage toGrpcSerializedPage(BlockEncodingSerde blockEncodingSerde, Page prestoPage)
    {
        PagesSerde pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
        SerializedPage serializedPage = pagesSerde.serialize(prestoPage);
        return GrpcSerializedPage.newBuilder()
                .setSliceBytes(ByteString.copyFrom(serializedPage.getSlice().getBytes()))
                .setPositionCount(serializedPage.getPositionCount())
                .setUncompressedSizeInBytes(serializedPage.getUncompressedSizeInBytes())
                .setPageCodecMarkers(ByteString.copyFrom(new byte[] {serializedPage.getPageCodecMarkers()}))
                .setChecksum(serializedPage.getChecksum())
                .build();
    }

    public static Page toPrestoPage(BlockEncodingSerde blockEncodingSerde, GrpcSerializedPage grpcSerializedPage)
    {
        PagesSerde pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
        return pagesSerde.deserialize(new SerializedPage(
                Slices.wrappedBuffer(grpcSerializedPage.getSliceBytes().toByteArray()),
                grpcSerializedPage.getPageCodecMarkers().byteAt(0),
                (int) grpcSerializedPage.getPositionCount(),
                (int) grpcSerializedPage.getUncompressedSizeInBytes(),
                grpcSerializedPage.getChecksum()));
    }

    public static GrpcUdfPage toGrpcUdfPage(GrpcUdfPageFormat grpcUdfPageFormat, GrpcSerializedPage grpcSerializedPage)
    {
        return GrpcUdfPage.newBuilder()
                .setGrpcUdfPageFormat(grpcUdfPageFormat)
                .setGrpcSerializedPage(grpcSerializedPage)
                .build();
    }
}
