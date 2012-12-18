package com.facebook.presto.event.scribe.client;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@ThriftStruct
public class SourceInfo
{
    private final String host;
    private final int port;
    private final long timestamp;

    @ThriftConstructor
    public SourceInfo(String host, int port, long timestamp)
    {
        checkNotNull(host, "host is null");
        checkArgument(port > 0, "port must be greater than zero");
        checkArgument(timestamp >= 0, "port must be at least zero");

        this.host = host;
        this.port = port;
        this.timestamp = timestamp;
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

    @ThriftField(3)
    public long getTimestamp()
    {
        return timestamp;
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

        SourceInfo that = (SourceInfo) o;

        if (port != that.port) {
            return false;
        }
        if (timestamp != that.timestamp) {
            return false;
        }
        if (host != null ? !host.equals(that.host) : that.host != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .add("timestamp", timestamp)
                .toString();
    }
}
