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

import com.facebook.presto.pinot.grpc.Constants;
import com.facebook.presto.pinot.grpc.GrpcRequestBuilder;
import com.facebook.presto.pinot.grpc.PinotStreamingQueryClient;
import com.facebook.presto.pinot.grpc.ServerResponse;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.MockPinotClusterInfoFetcher.DEFAULT_GRPC_PORT;

public class TestPinotSegmentStreamingPageSource
        extends TestPinotSegmentPageSource
{
    private static final class MockServerResponse
            extends ServerResponse
    {
        private DataTable dataTable;

        public MockServerResponse(Server.ServerResponse serverResponse)
        {
            super(serverResponse);
        }

        public MockServerResponse(DataTable dataTable)
        {
            super(null);
            this.dataTable = dataTable;
        }

        public String getResponseType()
        {
            return Constants.Response.ResponseType.DATA;
        }

        public int getSerializedSize()
        {
            return 0;
        }

        public ByteBuffer getPayloadReadOnlyByteBuffer()
        {
            try {
                return ByteBuffer.wrap(dataTable.toBytes());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public DataTable getDataTable(ByteBuffer byteBuffer) throws IOException
        {
            return SimpleDataTable.fromBytes(byteBuffer);
        }
    }

    private static final class MockPinotStreamingQueryClient
            extends PinotStreamingQueryClient
    {
        private final ImmutableList<DataTable> dataTables;

        MockPinotStreamingQueryClient(PinotStreamingQueryClient.Config pinotConfig, List<DataTable> dataTables)
        {
            super(pinotConfig);
            this.dataTables = ImmutableList.copyOf(dataTables);
        }

        @Override
        public Iterator<ServerResponse> submit(String host, int port, GrpcRequestBuilder requestBuilder)
        {
            return new Iterator<ServerResponse>()
            {
                int index;

                @Override
                public boolean hasNext()
                {
                    return index < dataTables.size();
                }

                @Override
                public ServerResponse next()
                {
                    return new MockServerResponse(dataTables.get(index++));
                }
            };
        }
    }

    @Override
    PinotSegmentPageSource getPinotSegmentPageSource(
            ConnectorSession session,
            List<DataTable> dataTables,
            PinotSplit mockPinotSplit,
            List<PinotColumnHandle> handlesSurviving)
    {
        MockPinotStreamingQueryClient mockPinotQueryClient = new MockPinotStreamingQueryClient(new PinotStreamingQueryClient.Config(pinotConfig.getStreamingServerGrpcMaxInboundMessageBytes(), true), dataTables);
        return new PinotSegmentStreamingPageSource(session, pinotConfig, mockPinotQueryClient, mockPinotSplit, handlesSurviving);
    }

    @Override
    Optional<Integer> getGrpcPort()
    {
        return Optional.of(DEFAULT_GRPC_PORT);
    }
}
