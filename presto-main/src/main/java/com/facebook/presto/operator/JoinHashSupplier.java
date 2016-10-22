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
package com.facebook.presto.operator;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class JoinHashSupplier
        implements Supplier<LookupSource>
{
    private final ConnectorSession session;
    private final PagesHash pagesHash;
    private final LongArrayList addresses;
    private final List<List<Block>> channels;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    public JoinHashSupplier(
            ConnectorSession session,
            PagesHashStrategy pagesHashStrategy,
            LongArrayList addresses,
            List<List<Block>> channels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory)
    {
        requireNonNull(session, "session is null");
        requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        requireNonNull(addresses, "addresses is null");
        requireNonNull(channels, "channels is null");
        requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");

        this.session = session;
        this.pagesHash = new PagesHash(addresses, pagesHashStrategy);
        this.addresses = addresses;
        this.channels = channels;
        this.filterFunctionFactory = filterFunctionFactory;
    }

    @Override
    public JoinHash get()
    {
        Optional<JoinFilterFunction> filterFunction = filterFunctionFactory.map(factory -> factory.create(session, addresses, channels));
        return new JoinHash(pagesHash, filterFunction);
    }
}
