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
package com.facebook.presto.execution.scheduler.faulttolerant;

import com.facebook.presto.spi.ConnectorId;
import com.google.common.collect.ImmutableSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.SizeOf.estimatedSizeOf;
import static com.facebook.presto.util.SizeOf.sizeOf;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class NodeRequirements
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NodeRequirements.class).instanceSize();

    private final Optional<ConnectorId> catalogName;
    private final Set<String> addresses;
    private final long retainedSizeInBytes;

    public NodeRequirements(Optional<ConnectorId> catalogName, Set<String> addresses)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.addresses = ImmutableSet.copyOf(requireNonNull(addresses, "addresses is null"));
        this.retainedSizeInBytes = INSTANCE_SIZE
                + sizeOf(catalogName, catalog -> estimatedSizeOf(catalog.toString()))
                + estimatedSizeOf(addresses, address -> estimatedSizeOf(address));
    }

    public Optional<ConnectorId> getCatalogName()
    {
        return catalogName;
    }

    public Set<String> getAddresses()
    {
        return addresses;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeRequirements that = (NodeRequirements) o;
        return Objects.equals(catalogName, that.catalogName)
                && Objects.equals(addresses, that.addresses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, addresses);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("addresses", addresses)
                .toString();
    }
}
