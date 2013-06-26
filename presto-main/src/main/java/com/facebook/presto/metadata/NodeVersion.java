package com.facebook.presto.metadata;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class NodeVersion
{
    public static final NodeVersion UNKNOWN = new NodeVersion("<unknown>");

    private final String version;

    public NodeVersion(String version)
    {
        this.version = checkNotNull(version, "version is null");
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

        NodeVersion that = (NodeVersion) o;
        return Objects.equal(version, that.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(version);
    }

    @Override
    public String toString()
    {
        return version;
    }
}
