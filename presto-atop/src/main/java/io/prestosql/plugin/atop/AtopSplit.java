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
package io.prestosql.plugin.atop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.time.Instant.ofEpochSecond;
import static java.util.Objects.requireNonNull;

public class AtopSplit
        implements ConnectorSplit
{
    private final AtopTable table;
    private final HostAddress host;
    private final ZonedDateTime date;

    @JsonCreator
    public AtopSplit(
            @JsonProperty("table") AtopTable table,
            @JsonProperty("host") HostAddress host,
            @JsonProperty("epochSeconds") long epochSeconds,
            @JsonProperty("timeZone") ZoneId timeZone)
    {
        this.table = requireNonNull(table, "table name is null");
        this.host = requireNonNull(host, "host is null");
        requireNonNull(timeZone, "timeZone is null");
        this.date = ZonedDateTime.ofInstant(ofEpochSecond(epochSeconds), timeZone);
    }

    @JsonProperty
    public AtopTable getTable()
    {
        return table;
    }

    @JsonProperty
    public HostAddress getHost()
    {
        return host;
    }

    @JsonProperty
    public long getEpochSeconds()
    {
        return date.toEpochSecond();
    }

    @JsonProperty
    public ZoneId getTimeZone()
    {
        return date.getZone();
    }

    public ZonedDateTime getDate()
    {
        return date;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        // discard the port number
        return ImmutableList.of(HostAddress.fromString(host.getHostText()));
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("host", host)
                .add("date", date)
                .toString();
    }
}
