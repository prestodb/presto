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
import io.prestosql.spi.HostAddress;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftHostAddress
{
    private final String host;
    private final int port;

    @ThriftConstructor
    public PrestoThriftHostAddress(String host, int port)
    {
        this.host = requireNonNull(host, "host is null");
        this.port = port;
    }

    @ThriftField(1)
    public String getHost()
    {
        return host;
    }

    @ThriftField(2)
    public int getPort()
    {
        return port;
    }

    public HostAddress toHostAddress()
    {
        return HostAddress.fromParts(getHost(), getPort());
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
        PrestoThriftHostAddress other = (PrestoThriftHostAddress) obj;
        return Objects.equals(this.host, other.host) &&
                this.port == other.port;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(host, port);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .toString();
    }
}
