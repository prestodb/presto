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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isFastInequalityJoin;
import static com.facebook.presto.operator.JoinUtils.channelsToPages;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JoinHashSupplier
        implements LookupSourceSupplier
{
    private final Session session;
    private final PagesHash pagesHash;
    private final AdaptiveLongBigArray addresses;
    private final List<Page> pages;
    private final Optional<PositionLinks.Factory> positionLinks;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    public JoinHashSupplier(
            Session session,
            PagesHashStrategy pagesHashStrategy,
            AdaptiveLongBigArray addresses,
            int positionCount,
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
                    positionCount,
                    pagesHashStrategy,
                    addresses);
        }
        else {
            positionLinksFactoryBuilder = ArrayPositionLinks.builder(positionCount);
        }

        this.pages = channelsToPages(channels);
        this.pagesHash = new PagesHash(addresses, positionCount, pagesHashStrategy, positionLinksFactoryBuilder);
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
                filterFunctionFactory.map(factory -> factory.create(session.getSqlFunctionProperties(), addresses, pages));
        return new JoinHash(
                pagesHash,
                filterFunction,
                positionLinks.map(links -> {
                    List<JoinFilterFunction> searchFunctions = searchFunctionFactories.stream()
                            .map(factory -> factory.create(session.getSqlFunctionProperties(), addresses, pages))
                            .collect(toImmutableList());
                    return links.create(searchFunctions);
                }));
    }
}
