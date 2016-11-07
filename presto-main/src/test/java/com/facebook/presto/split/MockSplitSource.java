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
package com.facebook.presto.split;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockSplitSource
        implements SplitSource
{
    private static final Split SPLIT = new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new MockConnectorSplit());

    private final int batchSize;
    private final int failAfter;
    private int remainingSplits;
    private int nextBatchCalls;

    public MockSplitSource(int batchSize, int totalSplits)
    {
        this(batchSize, totalSplits, Integer.MAX_VALUE);
    }

    public MockSplitSource(int batchSize, int totalSplits, int failAfter)
    {
        this.batchSize = batchSize;
        this.remainingSplits = totalSplits;
        this.failAfter = failAfter;
    }

    @Override
    public ConnectorId getConnectorId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<Split>> getNextBatch(int maxSize)
    {
        nextBatchCalls++;
        if (nextBatchCalls > failAfter) {
            throw new IllegalStateException("Mock failure");
        }
        int splits = Math.min(Math.min(batchSize, maxSize), remainingSplits);
        remainingSplits -= splits;
        return CompletableFuture.completedFuture(Collections.nCopies(splits, SPLIT));
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return remainingSplits == 0 || nextBatchCalls > failAfter;
    }

    public int getNextBatchCalls()
    {
        return nextBatchCalls;
    }

    public static class MockConnectorSplit
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return "A mock split";
        }
    }
}
