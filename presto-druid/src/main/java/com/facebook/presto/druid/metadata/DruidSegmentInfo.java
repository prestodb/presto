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
package com.facebook.presto.druid.metadata;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_METADATA_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidSegmentInfo
{
    private static final String DEEP_STORAGE_TYPE_KEY = "type";
    private static final String SEGMENT_PATH_KEY = "path";

    private final String dataSource;
    private final Interval interval;
    private final String version;
    private final Optional<Map<String, String>> loadSpec;
    private final Optional<Map<String, String>> shardSpec;
    private final Integer binaryVersion;
    private final long size;

    public enum DeepStorageType
    {
        HDFS("hdfs");

        private final String type;

        DeepStorageType(String type)
        {
            this.type = type;
        }
    }

    @JsonCreator
    public DruidSegmentInfo(
            @JsonProperty("dataSource") String dataSource,
            @JsonProperty("interval") Interval interval,
            @JsonProperty("version") String version,
            @JsonProperty("loadSpec") Optional<Map<String, String>> loadSpec,
            @JsonProperty("shardSpec") @Nullable Optional<Map<String, String>> shardSpec,
            @JsonProperty("binaryVersion") Integer binaryVersion,
            @JsonProperty("size") long size)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.interval = requireNonNull(interval, "interval is null");
        this.version = requireNonNull(version, "version is null");
        this.loadSpec = requireNonNull(loadSpec, "loadSpec is null");
        this.shardSpec = requireNonNull(shardSpec, "shardSpec is null");
        this.binaryVersion = requireNonNull(binaryVersion, "binaryVersion is null");
        this.size = requireNonNull(size, "size is null");
    }

    @JsonProperty
    public String getDataSource()
    {
        return dataSource;
    }

    @JsonProperty
    public Interval getInterval()
    {
        return interval;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @JsonProperty
    public Optional<Map<String, String>> getLoadSpec()
    {
        return loadSpec;
    }

    @JsonProperty
    public Optional<Map<String, String>> getShardSpec()
    {
        return shardSpec;
    }

    @JsonProperty
    public Integer getBinaryVersion()
    {
        return binaryVersion;
    }

    @JsonProperty
    public long getSize()
    {
        return size;
    }

    public DeepStorageType getDeepStorageType()
    {
        Map<String, String> loadSpec = getLoadSpec()
                .orElseThrow(() -> new PrestoException(DRUID_METADATA_ERROR, format("Malformed segment loadSpec: %s", getLoadSpec())));
        return DeepStorageType.valueOf(loadSpec.get(DEEP_STORAGE_TYPE_KEY).toUpperCase());
    }

    public String getDeepStoragePath()
    {
        Map<String, String> loadSpec = getLoadSpec()
                .orElseThrow(() -> new PrestoException(DRUID_METADATA_ERROR, format("Malformed segment loadSpec: %s", getLoadSpec())));
        return loadSpec.get(SEGMENT_PATH_KEY);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataSource, interval, version, loadSpec, shardSpec, binaryVersion, size);
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

        DruidSegmentInfo that = (DruidSegmentInfo) obj;
        return Objects.equals(this.dataSource, that.dataSource) &&
                Objects.equals(this.interval, that.interval) &&
                Objects.equals(this.version, that.version) &&
                Objects.equals(this.loadSpec, that.loadSpec) &&
                Objects.equals(this.shardSpec, that.shardSpec) &&
                Objects.equals(this.binaryVersion, that.binaryVersion) &&
                Objects.equals(this.size, that.size);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataSource", dataSource)
                .add("interval", interval)
                .add("version", version)
                .add("loadSpec", loadSpec)
                .add("shardSpec", shardSpec)
                .add("binaryVersion", binaryVersion)
                .add("size", size)
                .toString();
    }
}
