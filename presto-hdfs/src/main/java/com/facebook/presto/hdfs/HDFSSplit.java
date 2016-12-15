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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSSplit
implements ConnectorSplit
{
    private final HDFSConnectorId connectorId;
    private final SchemaTableName table;
    private final Path path;
    private final long start;
    private final long len;
    private final List<HostAddress> addresses;

    @JsonCreator
    public HDFSSplit(
            @JsonProperty("connectorId") HDFSConnectorId connectorId,
            @JsonProperty("table") SchemaTableName table,
            @JsonProperty("path") Path path,
            @JsonProperty("start") long start,
            @JsonProperty("len") long len,
            @JsonProperty("addresses") List<HostAddress> addresses
            )
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.table = requireNonNull(table, "table is null");
        this.path = requireNonNull(path, "path is null");
        this.start = requireNonNull(start);
        this.len = requireNonNull(len);
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    public HDFSConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getTable()
    {
        return table;
    }

    @JsonProperty
    public Path getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLen()
    {
        return len;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("connectorId", connectorId)
                .put("table", table)
                .put("path", path)
                .put("addresses", addresses)
                .build();
    }
}
