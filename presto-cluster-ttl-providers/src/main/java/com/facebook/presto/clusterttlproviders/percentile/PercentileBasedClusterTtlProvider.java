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

package com.facebook.presto.clusterttlproviders.percentile;

import com.facebook.presto.spi.ttl.ClusterTtlProvider;
import com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.google.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PercentileBasedClusterTtlProvider
        implements ClusterTtlProvider
{
    private final Comparator<ConfidenceBasedTtlInfo> ttlComparator = Comparator.comparing(ConfidenceBasedTtlInfo::getExpiryInstant);
    private final int percentile;

    @Inject
    public PercentileBasedClusterTtlProvider(PercentileBasedClusterTtlProviderConfig config)
    {
        requireNonNull(config, "config is null");
        checkArgument(config.getPercentile() > 0, "percentile should be greater than 0");
        this.percentile = config.getPercentile();
    }

    @Override
    public ConfidenceBasedTtlInfo getClusterTtl(List<NodeTtl> nodeTtls)
    {
        List<ConfidenceBasedTtlInfo> ttls = nodeTtls.stream()
                .map(this::getMinTtl)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .sorted(ttlComparator)
                .collect(toList());

        if (ttls.size() == 0) {
            return new ConfidenceBasedTtlInfo(0, 100);
        }

        return ttls.get((int) Math.ceil(ttls.size() * percentile / 100.0) - 1);
    }

    private Optional<ConfidenceBasedTtlInfo> getMinTtl(NodeTtl nodeTtl)
    {
        return nodeTtl.getTtlInfo().stream().min(ttlComparator);
    }
}
