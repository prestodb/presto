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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;

public class BlackHolePageSinkProvider
        implements ConnectorPageSinkProvider
{
    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return new NoOpConnectorPageSink();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return new NoOpConnectorPageSink();
    }

    private static class NoOpConnectorPageSink
            implements ConnectorPageSink
    {
        @Override
        public void appendPage(Page page, Block sampleWeightBlock)
        {
        }

        @Override
        public Collection<Slice> commit()
        {
            return ImmutableList.of();
        }

        @Override
        public void rollback()
        {
        }
    }
}
