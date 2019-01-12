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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.prestosql.spi.Page;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class TestOperatorAssertion
{
    private ScheduledExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newSingleThreadScheduledExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testToPagesWithBlockedOperator()
    {
        Operator operator = new BlockedOperator(Duration.valueOf("15 ms"));
        List<Page> pages = OperatorAssertion.toPages(operator, emptyIterator());
        Assert.assertEquals(pages, ImmutableList.of());
    }

    private class BlockedOperator
            implements Operator
    {
        private final Duration unblockAfter;
        private final OperatorContext operatorContext;

        private ListenableFuture<?> isBlocked = NOT_BLOCKED;

        public BlockedOperator(Duration unblockAfter)
        {
            this.unblockAfter = requireNonNull(unblockAfter, "unblockAfter is null");
            this.operatorContext = TestingOperatorContext.create(executor);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return isBlocked;
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void finish()
        {
            if (this.isBlocked == NOT_BLOCKED) {
                SettableFuture<?> isBlocked = SettableFuture.create();
                this.isBlocked = isBlocked;
                executor.schedule(() -> isBlocked.set(null), unblockAfter.toMillis(), TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public boolean isFinished()
        {
            return isBlocked != NOT_BLOCKED // finish() not called yet
                    && isBlocked.isDone();
        }

        @Override
        public Page getOutput()
        {
            return null;
        }
    }
}
