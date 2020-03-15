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
package com.facebook.presto.cache;

import org.apache.hadoop.fs.Path;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FileReadRequest
{
    private final Path path;
    private final long offset;
    private final int length;

    public FileReadRequest(Path path, long offset, int length)
    {
        this.path = requireNonNull(path, "path is null");
        this.offset = requireNonNull(offset, "offset is null");
        this.length = requireNonNull(length, "length is null");
    }

    public Path getPath()
    {
        return path;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, offset, length);
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FileReadRequest)) {
            return false;
        }
        FileReadRequest other = (FileReadRequest) object;
        return Objects.equals(this.path, other.path) &&
                Objects.equals(this.offset, other.offset) &&
                Objects.equals(this.length, other.length);
    }
}
