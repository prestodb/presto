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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("deprecation")
public abstract class ForwardingFileStatus
        extends FileStatus
{
    private final FileStatus fileStatus;

    protected ForwardingFileStatus(FileStatus fileStatus)
    {
        this.fileStatus = checkNotNull(fileStatus, "fileStatus is null");
    }

    @Override
    public long getLen()
    {
        return fileStatus.getLen();
    }

    @Override
    public boolean isDir()
    {
        return fileStatus.isDir();
    }

    @Override
    public long getBlockSize()
    {
        return fileStatus.getBlockSize();
    }

    @Override
    public short getReplication()
    {
        return fileStatus.getReplication();
    }

    @Override
    public long getModificationTime()
    {
        return fileStatus.getModificationTime();
    }

    @Override
    public long getAccessTime()
    {
        return fileStatus.getAccessTime();
    }

    @Override
    public FsPermission getPermission()
    {
        return fileStatus.getPermission();
    }

    @Override
    public String getOwner()
    {
        return fileStatus.getOwner();
    }

    @Override
    public String getGroup()
    {
        return fileStatus.getGroup();
    }

    @Override
    public Path getPath()
    {
        return fileStatus.getPath();
    }

    @Override
    public void write(DataOutput out)
            throws IOException
    {
        fileStatus.write(out);
    }

    @Override
    public void readFields(DataInput in)
            throws IOException
    {
        fileStatus.readFields(in);
    }

    @Override
    public int compareTo(Object o)
    {
        return fileStatus.compareTo(o);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o)
    {
        return (o == this) || fileStatus.equals(o);
    }

    @Override
    public int hashCode()
    {
        return fileStatus.hashCode();
    }

    @SuppressWarnings("ObjectToString")
    @Override
    public String toString()
    {
        return fileStatus.toString();
    }
}
