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
package com.facebook.presto.spi.ttl;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestingNodeTtlFetcherFactory
        implements NodeTtlFetcherFactory
{
    private final Map<NodeInfo, NodeTtl> ttlInfo;

    public TestingNodeTtlFetcherFactory(Map<NodeInfo, NodeTtl> ttlInfo)
    {
        this.ttlInfo = requireNonNull(ttlInfo, "ttlInfo is null");
    }

    @Override
    public String getName()
    {
        return "testing-node-ttl-fetcher";
    }

    @Override
    public NodeTtlFetcher create(Map<String, String> config)
    {
        return new TestingNodeTtlFetcher(ttlInfo);
    }
}
