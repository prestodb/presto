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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSplit
{
    private final PrestoThriftId splitId;
    private final List<PrestoThriftHostAddress> hosts;

    @ThriftConstructor
    public PrestoThriftSplit(PrestoThriftId splitId, List<PrestoThriftHostAddress> hosts)
    {
        this.splitId = requireNonNull(splitId, "splitId is null");
        this.hosts = requireNonNull(hosts, "hosts is null");
    }

    /**
     * Encodes all the information needed to identify a batch of rows to return to Presto.
     * For a basic scan, includes schema name, table name, and output constraint.
     * For an index scan, includes schema name, table name, set of keys to lookup and output constraint.
     */
    @ThriftField(1)
    public PrestoThriftId getSplitId()
    {
        return splitId;
    }

    /**
     * Identifies the set of hosts on which the rows are available. If empty, then the rows
     * are expected to be available on any host. The hosts in this list may be independent
     * from the hosts used to serve metadata requests.
     */
    @ThriftField(2)
    public List<PrestoThriftHostAddress> getHosts()
    {
        return hosts;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftSplit other = (PrestoThriftSplit) obj;
        return Objects.equals(this.splitId, other.splitId) &&
                Objects.equals(this.hosts, other.hosts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(splitId, hosts);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitId", splitId)
                .add("hosts", hosts)
                .toString();
    }
}
