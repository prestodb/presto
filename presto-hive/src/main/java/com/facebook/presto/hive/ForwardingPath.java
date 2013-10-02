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
package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("deprecation")
public abstract class ForwardingPath
        extends Path
{
    private final Path path;

    protected ForwardingPath(Path path)
    {
        super(checkNotNull(path, "path is null").toString());
        this.path = path;
    }

    @Override
    public URI toUri()
    {
        return path.toUri();
    }

    @Override
    public FileSystem getFileSystem(Configuration conf)
            throws IOException
    {
        return path.getFileSystem(conf);
    }

    @Override
    public boolean isAbsolute()
    {
        return path.isAbsolute();
    }

    @Override
    public String getName()
    {
        return path.getName();
    }

    @Override
    public Path getParent()
    {
        return path.getParent();
    }

    @Override
    public Path suffix(String suffix)
    {
        return path.suffix(suffix);
    }

    @Override
    public String toString()
    {
        return path.toString();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o)
    {
        return (o == this) || path.equals(o);
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }

    @Override
    public int compareTo(Object o)
    {
        return path.compareTo(o);
    }

    @Override
    public int depth()
    {
        return path.depth();
    }

    @Override
    public Path makeQualified(FileSystem fs)
    {
        return path.makeQualified(fs);
    }
}
