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
package com.facebook.presto.clusterttlproviders;

import com.facebook.presto.clusterttlproviders.percentile.PercentileBasedClusterTtlProvider;
import com.facebook.presto.clusterttlproviders.percentile.PercentileBasedClusterTtlProviderConfig;
import com.facebook.presto.spi.ttl.ClusterTtlProvider;
import com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestPercentileBasedClusterTtlProvider
{
    private final ClusterTtlProvider clusterTtlProvider = new PercentileBasedClusterTtlProvider(
            new PercentileBasedClusterTtlProviderConfig().setPercentile(1));

    @Test
    public void testWithEmptyTtlList()
    {
        assertEquals(clusterTtlProvider.getClusterTtl(ImmutableList.of()), new ConfidenceBasedTtlInfo(0, 100));
    }

    @Test
    public void testWithNonEmptyTtlList()
    {
        List<NodeTtl> nodeTtls = ImmutableList.of(new NodeTtl(ImmutableSet.of(new ConfidenceBasedTtlInfo(10, 100))),
                new NodeTtl(ImmutableSet.of(new ConfidenceBasedTtlInfo(30, 100))));
        assertEquals(clusterTtlProvider.getClusterTtl(nodeTtls), new ConfidenceBasedTtlInfo(10, 100));
    }
}
