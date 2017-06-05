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

import com.facebook.presto.Session;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isFastInequalityJoin;
import static java.util.Objects.requireNonNull;

public class JoinHashSupplier
        implements LookupSourceSupplier
{
    private final Session session;
    private final PagesHash pagesHash;
    private final LongArrayList addresses;
    private final List<List<Block>> channels;
    private final Optional<PositionLinks.Factory> positionLinks;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    public JoinHashSupplier(
            Session session,
            PagesHashStrategy pagesHashStrategy,
            LongArrayList addresses,
            List<List<Block>> channels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory)
    {
        this.session = requireNonNull(session, "session is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.channels = requireNonNull(channels, "channels is null");
        this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
        requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");

        PositionLinks.FactoryBuilder positionLinksFactoryBuilder;
        if (filterFunctionFactory.isPresent() &&
                filterFunctionFactory.get().getSortChannel().isPresent() &&
                isFastInequalityJoin(session)) {
            positionLinksFactoryBuilder = SortedPositionLinks.builder(
                    addresses.size(),
                    pagesHashStrategy,
                    addresses);
        }
        else {
            positionLinksFactoryBuilder = ArrayPositionLinks.builder(addresses.size());
        }

        this.pagesHash = new PagesHash(addresses, pagesHashStrategy, positionLinksFactoryBuilder);
        this.positionLinks = positionLinksFactoryBuilder.isEmpty() ? Optional.empty() : Optional.of(positionLinksFactoryBuilder.build());
    }

    @Override
    public long getHashCollisions()
    {
        return pagesHash.getHashCollisions();
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return pagesHash.getExpectedHashCollisions();
    }

    @Override
    public JoinHash get()
    {
        // We need to create new JoinFilterFunction per each thread using it, since those functions
        // are not thread safe...
        Optional<JoinFilterFunction> filterFunction =
                filterFunctionFactory.map(factory -> factory.create(session.toConnectorSession(), addresses, channels));
        return new JoinHash(
                pagesHash,
                filterFunction,
                positionLinks.map(links -> links.create(filterFunction)));
    }
}
