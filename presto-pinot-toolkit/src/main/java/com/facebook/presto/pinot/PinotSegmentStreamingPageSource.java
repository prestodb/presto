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
package com.facebook.presto.pinot;

import com.facebook.presto.common.Page;
import com.facebook.presto.pinot.grpc.Constants;
import com.facebook.presto.pinot.grpc.GrpcRequestBuilder;
import com.facebook.presto.pinot.grpc.PinotStreamingQueryClient;
import com.facebook.presto.pinot.grpc.ServerResponse;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.pinot.common.utils.DataTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_DATA_FETCH_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INVALID_SEGMENT_QUERY_GENERATED;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static java.util.Objects.requireNonNull;

/**
 * This class retrieves Pinot data from a Pinot client, and re-constructs the data into Presto Pages.
 */
public class PinotSegmentStreamingPageSource
        extends PinotSegmentPageSource
{
    private final PinotStreamingQueryClient pinotStreamingQueryClient;
    private Iterator<ServerResponse> serverResponseIterator;
    private long completedPositions;

    public PinotSegmentStreamingPageSource(
            ConnectorSession session,
            PinotConfig pinotConfig,
            PinotStreamingQueryClient pinotStreamingQueryClient,
            PinotSplit split,
            List<PinotColumnHandle> columnHandles)
    {
        super(session, pinotConfig, null, split, columnHandles);
        this.pinotStreamingQueryClient = requireNonNull(pinotStreamingQueryClient, "pinotStreamingQueryClient is null");
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    /**
     * @return constructed page for pinot data.
     */
    @Override
    public Page getNextPage()
    {
        if (closed) {
            return null;
        }

        if (serverResponseIterator == null) {
            serverResponseIterator = queryPinot(split);
        }
        ByteBuffer byteBuffer = null;
        try {
            // Pinot gRPC server response iterator returns:
            //   - n data blocks based on inbound message size;
            //   - 1 metadata of the query results.
            // So we need to check ResponseType of each ServerResponse.
            if (serverResponseIterator.hasNext()) {
                long startTimeNanos = System.nanoTime();
                ServerResponse serverResponse = serverResponseIterator.next();
                readTimeNanos += System.nanoTime() - startTimeNanos;
                switch (serverResponse.getResponseType()) {
                    case Constants.Response.ResponseType.DATA:
                        estimatedMemoryUsageInBytes = serverResponse.getSerializedSize();
                        // Store each dataTable which will later be constructed into Pages.
                        try {
                            byteBuffer = serverResponse.getPayloadReadOnlyByteBuffer();
                            DataTable dataTable = serverResponse.getDataTable(byteBuffer);
                            checkExceptions(dataTable, split, PinotSessionProperties.isMarkDataFetchExceptionsAsRetriable(session));
                            currentDataTable = new PinotSegmentPageSource.PinotDataTableWithSize(dataTable, serverResponse.getSerializedSize());
                        }
                        catch (IOException e) {
                            throw new PinotException(
                                    PINOT_DATA_FETCH_EXCEPTION,
                                    split.getSegmentPinotQuery(),
                                    String.format("Encountered Pinot exceptions when fetching data table from Split: < %s >", split),
                                    e);
                        }
                        break;
                    case Constants.Response.ResponseType.METADATA:
                        // The last part of the response is Metadata
                        currentDataTable = null;
                        serverResponseIterator = null;
                        close();
                        return null;
                    default:
                        throw new PinotException(
                                PINOT_UNEXPECTED_RESPONSE,
                                split.getSegmentPinotQuery(),
                                String.format("Encountered Pinot exceptions, unknown response type - %s", serverResponse.getResponseType()));
                }
            }
            Page page = fillNextPage();
            completedPositions += currentDataTable.getDataTable().getNumberOfRows();
            return page;
        }
        finally {
            if (byteBuffer != null) {
                byteBuffer.clear();
            }
        }
    }

    private Iterator<ServerResponse> queryPinot(PinotSplit split)
    {
        String sql = split.getSegmentPinotQuery().orElseThrow(() -> new PinotException(PINOT_INVALID_SEGMENT_QUERY_GENERATED, Optional.empty(), "Expected the segment split to contain the pinot query"));
        String grpcHost = split.getGrpcHost().orElseThrow(() -> new PinotException(PINOT_INVALID_SEGMENT_QUERY_GENERATED, Optional.empty(), "Expected the segment split to contain the grpc host"));
        int grpcPort = split.getGrpcPort().orElseThrow(() -> new PinotException(PINOT_INVALID_SEGMENT_QUERY_GENERATED, Optional.empty(), "Expected the segment split to contain the grpc port"));
        if (grpcPort <= 0) {
            throw new PinotException(
                    PINOT_INVALID_SEGMENT_QUERY_GENERATED,
                    Optional.empty(),
                    "Expected the grpc port > 0 always");
        }
        return pinotStreamingQueryClient.submit(grpcHost, grpcPort, new GrpcRequestBuilder()
                .setSegments(split.getSegments())
                .setEnableStreaming(true)
                .setBrokerId("presto-coordinator-grpc")
                .setSql(sql));
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }
}
