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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.exchange.LocalMergeExchange.ExchangeBuffer;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestLocalMergeExchange
{
    private static final Page PAGE = rowPagesBuilder(ImmutableList.of(INTEGER))
            .row(1)
            .build()
            .get(0);

    @Test
    public void testWriteIsBlocked()
            throws Exception
    {
        LocalMergeExchange exchange = new LocalMergeExchange(1, 2);
        ExchangeBuffer buffer = exchange.getBuffer(0);
        assertTrue(buffer.isWriteBlocked().isDone());
        buffer.enqueuePage(PAGE);
        assertTrue(buffer.isWriteBlocked().isDone());
        buffer.enqueuePage(PAGE);
        ListenableFuture<?> blocked = buffer.isWriteBlocked();
        assertFalse(blocked.isDone());
        assertNotNull(buffer.poolPage());
        assertTrue(blocked.isDone());
    }

    @Test
    public void testReadIsBlocked()
            throws Exception
    {
        LocalMergeExchange exchange = new LocalMergeExchange(1, 2);
        ExchangeBuffer buffer = exchange.getBuffer(0);
        ListenableFuture<?> blocked = buffer.isReadBlocked();
        assertFalse(blocked.isDone());
        buffer.enqueuePage(PAGE);
        assertTrue(blocked.isDone());
        assertNotNull(buffer.poolPage());
        assertFalse(buffer.isReadBlocked().isDone());
    }

    @Test
    public void testFinishWrite()
            throws Exception
    {
        LocalMergeExchange exchange = new LocalMergeExchange(1, 2);
        ExchangeBuffer buffer = exchange.getBuffer(0);
        ListenableFuture<?> blocked = buffer.isReadBlocked();
        assertFalse(blocked.isDone());
        buffer.finishWrite();
        assertTrue(buffer.isWriteFinished());
        assertTrue(blocked.isDone());
        assertNull(buffer.poolPage());
    }

    @Test
    public void testFinishRead()
            throws Exception
    {
        LocalMergeExchange exchange = new LocalMergeExchange(2, 1);

        ExchangeBuffer buffer1 = exchange.getBuffer(0);
        ExchangeBuffer buffer2 = exchange.getBuffer(1);

        buffer1.enqueuePage(PAGE);
        buffer2.enqueuePage(PAGE);

        ListenableFuture<?> buffer1Blocked = buffer1.isWriteBlocked();
        assertFalse(buffer1Blocked.isDone());
        ListenableFuture<?> buffer2Blocked = buffer2.isWriteBlocked();
        assertFalse(buffer2Blocked.isDone());

        exchange.finishRead();
        assertTrue(buffer1.isReadFinished());
        assertTrue(buffer2.isReadFinished());
        assertTrue(buffer1Blocked.isDone());
        assertTrue(buffer2Blocked.isDone());
        assertTrue(buffer1.isReadBlocked().isDone());
        assertTrue(buffer1.isWriteBlocked().isDone());
        assertTrue(buffer2.isReadBlocked().isDone());
        assertTrue(buffer2.isWriteBlocked().isDone());
    }
}
