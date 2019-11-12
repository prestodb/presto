package com.facebook.presto.cache;

import java.util.Objects;

public class FileToken
{
    private final String path;

    public FileToken(String path)
    {
        this.path = path;
    }

    @Override
    public String toString()
    {
        return "FileToken:" + path;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        FileToken fileToken = (FileToken) o;
        return path.equals(fileToken.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }
}
