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
package com.facebook.presto.execution;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TestingPageSourceProvider
        implements ConnectorPageSourceProvider
{
    public TestingPageSourceProvider()
    {
        System.out.println();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        ImmutableList<Block> blocks = columns.stream()
                .map(column -> new FixedWidthBlockBuilder(0, 1).appendNull().build())
                .collect(toImmutableList());

        return new FixedPageSource(ImmutableList.of(new Page(blocks.toArray(new Block[blocks.size()]))));
    }
}
