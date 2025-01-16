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
package com.facebook.presto.ttl.nodettlfetchermanagers;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;

import java.util.Map;
import java.util.Optional;

public class ThrowingNodeTtlFetcherManager
        implements NodeTtlFetcherManager
{
    @Override
    public Optional<NodeTtl> getTtlInfo(InternalNode node)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<InternalNode, NodeTtl> getAllTtls()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addNodeTtlFetcherFactory(NodeTtlFetcherFactory factory)
    {
    }

    @Override
    public void loadNodeTtlFetcher()
    {
    }
}
