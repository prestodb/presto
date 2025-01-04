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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class ArrowPageSource
        implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ArrowPageSource.class);
    private final ArrowSplit split;
    private final List<ArrowColumnHandle> columnHandles;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private final FlightStream flightStream;
    private final ClientClosingFlightStream flightStreamAndClient;
    private boolean completed;
    private int currentPosition;
    private VectorSchemaRoot vectorSchemaRoot;

    public ArrowPageSource(
            ArrowSplit split,
            List<ArrowColumnHandle> columnHandles,
            BaseArrowFlightClient clientHandler,
            ConnectorSession connectorSession,
            ArrowBlockBuilder arrowBlockBuilder)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.split = requireNonNull(split, "split is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowBlockBuilder is null");
        flightStreamAndClient = clientHandler.getFlightStream(split, getTicket(ByteBuffer.wrap(split.getFlightEndpoint())), connectorSession);
        flightStream = flightStreamAndClient.getFlightStream();
    }

    private Ticket getTicket(ByteBuffer byteArray)
    {
        try {
            return FlightEndpoint.deserialize(byteArray).getTicket();
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return currentPosition;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return completed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public Page getNextPage()
    {
        if (flightStream.next()) {
            vectorSchemaRoot = flightStream.getRoot();
        }
        else {
            completed = true;
            return null;
        }

        currentPosition = currentPosition + 1;

        List<Block> blocks = new ArrayList<>();
        for (int columnIndex = 0; columnIndex < columnHandles.size(); columnIndex++) {
            FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
            Type type = columnHandles.get(columnIndex).getColumnType();
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(vector, type, flightStream.getDictionaryProvider());
            blocks.add(block);
        }

        return new Page(vectorSchemaRoot.getRowCount(), blocks.toArray(new Block[0]));
    }

    @Override
    public void close()
    {
        if (vectorSchemaRoot != null) {
            vectorSchemaRoot.close();
            completed = true;
        }

        try {
            flightStreamAndClient.close();
        }
        catch (Exception e) {
            logger.error(e, "Error closing flight stream");
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, e.getMessage(), e);
        }
    }
}
