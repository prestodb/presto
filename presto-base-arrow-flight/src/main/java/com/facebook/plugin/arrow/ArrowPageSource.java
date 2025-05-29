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
import org.apache.arrow.vector.FieldVector;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class ArrowPageSource
        implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ArrowPageSource.class);
    private final List<ArrowColumnHandle> columnHandles;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private final ClientClosingFlightStream flightStreamAndClient;
    private boolean completed;
    private int currentPosition;

    public ArrowPageSource(
            ArrowSplit split,
            List<ArrowColumnHandle> columnHandles,
            BaseArrowFlightClientHandler clientHandler,
            ConnectorSession connectorSession,
            ArrowBlockBuilder arrowBlockBuilder)
    {
        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(clientHandler, "clientHandler is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowBlockBuilder is null");
        this.flightStreamAndClient = clientHandler.getFlightStream(connectorSession, split);
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
        logger.debug("Reading next Arrow record batch");

        if (!flightStreamAndClient.next()) {
            // No more streams, end pages
            completed = true;
            logger.debug("Finished reading Arrow record batches");
            return null;
        }

        currentPosition = currentPosition + 1;

        // Create blocks from the loaded Arrow record batch
        List<Block> blocks = new ArrayList<>();
        List<FieldVector> vectors = flightStreamAndClient.getRoot().getFieldVectors();
        for (int columnIndex = 0; columnIndex < columnHandles.size(); columnIndex++) {
            FieldVector vector = vectors.get(columnIndex);
            Type type = columnHandles.get(columnIndex).getColumnType();
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(vector, type, flightStreamAndClient.getDictionaryProvider());
            blocks.add(block);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Read Arrow record batch with rows: %s, columns: %s", flightStreamAndClient.getRoot().getRowCount(), vectors.size());
        }

        return new Page(flightStreamAndClient.getRoot().getRowCount(), blocks.toArray(new Block[0]));
    }

    @Override
    public void close()
    {
        try {
            flightStreamAndClient.close();
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, e.getMessage(), e);
        }
    }
}
