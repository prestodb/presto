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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class SyntheticHiveFileInfo
{
    private final String path;
    private final long length;
    private final long modificationTime;

    @JsonCreator
    public SyntheticHiveFileInfo(
            @JsonProperty("path") String path,
            @JsonProperty("length") long length,
            @JsonProperty("modificationTime") long modificationTime)
    {
        this.path = requireNonNull(path, "path is null");
        this.length = length;
        this.modificationTime = modificationTime;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getModificationTime()
    {
        return modificationTime;
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
        SyntheticHiveFileInfo that = (SyntheticHiveFileInfo) o;
        return length == that.length && modificationTime == that.modificationTime && path.equals(that.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, length, modificationTime);
    }
}
