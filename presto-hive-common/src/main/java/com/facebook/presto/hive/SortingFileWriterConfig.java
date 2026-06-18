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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.MaxDataSize;
import com.facebook.airlift.units.MinDataSize;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;

public class SortingFileWriterConfig
{
    private DataSize writerSortBufferSize = new DataSize(64, MEGABYTE);
    private int maxOpenSortFiles = 50;

    @MinDataSize("1MB")
    @MaxDataSize("1GB")
    public DataSize getWriterSortBufferSize()
    {
        return writerSortBufferSize;
    }

    @Config("writer-sort-buffer-size")
    @ConfigDescription("Defines how much memory is used for this in-memory sorting process.")
    public SortingFileWriterConfig setWriterSortBufferSize(DataSize writerSortBufferSize)
    {
        this.writerSortBufferSize = writerSortBufferSize;
        return this;
    }

    @Min(2)
    @Max(1000)
    public int getMaxOpenSortFiles()
    {
        return maxOpenSortFiles;
    }

    @Config("max-open-sort-files")
    @ConfigDescription("When writing, the maximum number of temporary files opened at one time to write sorted data.")
    public SortingFileWriterConfig setMaxOpenSortFiles(int maxOpenSortFiles)
    {
        this.maxOpenSortFiles = maxOpenSortFiles;
        return this;
    }
}
