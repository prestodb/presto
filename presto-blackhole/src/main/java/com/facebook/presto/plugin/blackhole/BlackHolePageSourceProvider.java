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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.plugin.blackhole.Types.checkType;

public final class BlackHolePageSourceProvider
        implements ConnectorPageSourceProvider
{
    @Override
    public ConnectorPageSource createPageSource(ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        BlackHoleSplit blackHoleSplit = checkType(split, BlackHoleSplit.class, "BlackHoleSplit");

        ImmutableList.Builder<Type> builder = ImmutableList.builder();

        for (ColumnHandle column : columns) {
            builder.add((checkType(column, BlackHoleColumnHandle.class, "BlackHoleColumnHandle")).getColumnType());
        }
        List<Type> types = builder.build();

        return new RepeatingPageSource(
                generateZeroPage(types, blackHoleSplit.getRowsPerPage()),
                blackHoleSplit.getPagesCount());
    }

    private Page generateZeroPage(List<Type> types, int rowsCount)
    {
        ConnectorPageSource pageSource = new RecordPageSource(types, new BlackHoleRecordCursor(types, rowsCount));
        return pageSource.getNextPage();
    }
}
