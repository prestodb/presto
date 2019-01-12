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
import io.prestosql.Session;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.isFastInequalityJoin;
import static io.prestosql.operator.JoinUtils.channelsToPages;
import static java.util.Objects.requireNonNull;

public class JoinHashSupplier
        implements LookupSourceSupplier
{
    private final Session session;
    private final PagesHash pagesHash;
    private final LongArrayList addresses;
    private final List<Page> pages;
    private final Optional<PositionLinks.Factory> positionLinks;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    public JoinHashSupplier(
            Session session,
            PagesHashStrategy pagesHashStrategy,
            LongArrayList addresses,
            List<List<Block>> channels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories)
    {
        this.session = requireNonNull(session, "session is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
        this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
        requireNonNull(channels, "pages is null");
        requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");

        PositionLinks.FactoryBuilder positionLinksFactoryBuilder;
        if (sortChannel.isPresent() &&
                isFastInequalityJoin(session)) {
            checkArgument(filterFunctionFactory.isPresent(), "filterFunctionFactory not set while sortChannel set");
            positionLinksFactoryBuilder = SortedPositionLinks.builder(
                    addresses.size(),
                    pagesHashStrategy,
                    addresses);
        }
        else {
            positionLinksFactoryBuilder = ArrayPositionLinks.builder(addresses.size());
        }

        this.pages = channelsToPages(channels);
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
    public long checksum()
    {
        return positionLinks.map(PositionLinks.Factory::checksum).orElse(0L);
    }

    @Override
    public JoinHash get()
    {
        // We need to create new JoinFilterFunction per each thread using it, since those functions
        // are not thread safe...
        Optional<JoinFilterFunction> filterFunction =
                filterFunctionFactory.map(factory -> factory.create(session.toConnectorSession(), addresses, pages));
        return new JoinHash(
                pagesHash,
                filterFunction,
                positionLinks.map(links -> {
                    List<JoinFilterFunction> searchFunctions = searchFunctionFactories.stream()
                            .map(factory -> factory.create(session.toConnectorSession(), addresses, pages))
                            .collect(toImmutableList());
                    return links.create(searchFunctions);
                }));
    }
}
