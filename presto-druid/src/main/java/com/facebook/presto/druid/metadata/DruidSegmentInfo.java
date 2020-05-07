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
    private final String version;
    private final Optional<Map<String, String>> loadSpecification;
    private final Optional<Map<String, Object>> shardSpecification;
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
            @JsonProperty("version") String version,
            @JsonProperty("loadSpec") Optional<Map<String, String>> loadSpecification,
            @JsonProperty("shardSpec") @Nullable Optional<Map<String, Object>> shardSpecification,
            @JsonProperty("binaryVersion") Integer binaryVersion,
            @JsonProperty("size") long size)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.version = requireNonNull(version, "version is null");
        this.loadSpecification = requireNonNull(loadSpecification, "loadSpecification is null");
        this.shardSpecification = requireNonNull(shardSpecification, "shardSpecification is null");
        this.binaryVersion = requireNonNull(binaryVersion, "binaryVersion is null");
        this.size = size;
    }

    @JsonProperty
    public String getDataSource()
    {
        return dataSource;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @JsonProperty
    public Optional<Map<String, String>> getLoadSpecification()
    {
        return loadSpecification;
    }

    @JsonProperty
    public Optional<Map<String, Object>> getShardSpecification()
    {
        return shardSpecification;
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
        Map<String, String> loadSpecification = getLoadSpecification()
                .orElseThrow(() -> new PrestoException(DRUID_METADATA_ERROR, format("Malformed segment loadSpecification: %s", getLoadSpecification())));
        return DeepStorageType.valueOf(loadSpecification.get(DEEP_STORAGE_TYPE_KEY).toUpperCase());
    }

    public String getDeepStoragePath()
    {
        Map<String, String> loadSpecification = getLoadSpecification()
                .orElseThrow(() -> new PrestoException(DRUID_METADATA_ERROR, format("Malformed segment loadSpecification: %s", getLoadSpecification())));
        return loadSpecification.get(SEGMENT_PATH_KEY);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataSource, version, loadSpecification, shardSpecification, binaryVersion, size);
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
                Objects.equals(this.version, that.version) &&
                Objects.equals(this.loadSpecification, that.loadSpecification) &&
                Objects.equals(this.shardSpecification, that.shardSpecification) &&
                Objects.equals(this.binaryVersion, that.binaryVersion) &&
                Objects.equals(this.size, that.size);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataSource", dataSource)
                .add("version", version)
                .add("loadSpecification", loadSpecification)
                .add("shardSpecification", shardSpecification)
                .add("binaryVersion", binaryVersion)
                .add("size", size)
                .toString();
    }
}
