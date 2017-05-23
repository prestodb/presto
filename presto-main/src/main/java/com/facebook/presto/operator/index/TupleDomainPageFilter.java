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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.operator.project.PageFilter.positionsArrayToSelectedPositions;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TupleDomainPageFilter
        implements PageFilter
{
    private final InputChannels inputChannels;
    private final List<Integer> filterChannels;
    @Nullable
    private final Domain[] domains;

    private boolean[] selectedPositions = new boolean[0];

    public TupleDomainPageFilter(InputChannels inputChannels, List<Integer> filterChannels, TupleDomain<Integer> tupleDomain)
    {
        this(inputChannels, filterChannels, getDomains(inputChannels, requireNonNull(tupleDomain, "tupleDomain is null")));
    }

    private TupleDomainPageFilter(InputChannels inputChannels, List<Integer> filterChannels, @Nullable Domain[] domains)
    {
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.filterChannels = requireNonNull(filterChannels, "filterChannels is null");
        this.domains = domains;
    }

    @Nullable
    private static Domain[] getDomains(InputChannels inputChannels, TupleDomain<Integer> tupleDomain)
    {
        if (tupleDomain.getDomains().isPresent()) {
            Domain[] domains = new Domain[inputChannels.size()];
            for (int i = 0; i < domains.length; i++) {
                domains[i] = tupleDomain.getDomains().get().get(i);
            }
            return domains;
        }
        return null;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        if (domains == null) {
            return positionsRange(0, page.getPositionCount());
        }

        if (selectedPositions.length < page.getPositionCount()) {
            selectedPositions = new boolean[page.getPositionCount()];
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            selectedPositions[position] = matches(page, position);
        }

        return positionsArrayToSelectedPositions(selectedPositions, page.getPositionCount());
    }

    private boolean matches(Page page, int position)
    {
        for (int channel : filterChannels) {
            if (!matches(page, position, channel)) {
                return false;
            }
        }
        return true;
    }

    private boolean matches(Page page, int position, int channel)
    {
        checkState(domains != null, "domains is null");
        Domain domain = domains[channel];
        if (domain == null) {
            return true;
        }
        return domain.includesNullableValue(page.getBlock(channel), position);
    }
}
