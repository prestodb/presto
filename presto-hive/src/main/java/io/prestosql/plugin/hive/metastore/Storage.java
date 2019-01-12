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
package io.prestosql.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveBucketProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Storage
{
    private final StorageFormat storageFormat;
    private final String location;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final boolean skewed;
    private final Map<String, String> serdeParameters;

    @JsonCreator
    public Storage(
            @JsonProperty("storageFormat") StorageFormat storageFormat,
            @JsonProperty("location") String location,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("skewed") boolean skewed,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters)
    {
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.location = requireNonNull(location, "location is null");
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.skewed = skewed;
        this.serdeParameters = ImmutableMap.copyOf(requireNonNull(serdeParameters, "serdeParameters is null"));
    }

    @JsonProperty
    public StorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public Optional<HiveBucketProperty> getBucketProperty()
    {
        return bucketProperty;
    }

    @JsonProperty
    public boolean isSkewed()
    {
        return skewed;
    }

    @JsonProperty
    public Map<String, String> getSerdeParameters()
    {
        return serdeParameters;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("skewed", skewed)
                .add("storageFormat", storageFormat)
                .add("location", location)
                .add("bucketProperty", bucketProperty)
                .add("serdeParameters", serdeParameters)
                .toString();
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

        Storage storage = (Storage) o;
        return skewed == storage.skewed &&
                Objects.equals(storageFormat, storage.storageFormat) &&
                Objects.equals(location, storage.location) &&
                Objects.equals(bucketProperty, storage.bucketProperty) &&
                Objects.equals(serdeParameters, storage.serdeParameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(skewed, storageFormat, location, bucketProperty, serdeParameters);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Storage storage)
    {
        return new Builder(storage);
    }

    public static class Builder
    {
        private StorageFormat storageFormat;
        private String location;
        private Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        private boolean skewed;
        private Map<String, String> serdeParameters = ImmutableMap.of();

        private Builder()
        {
        }

        private Builder(Storage storage)
        {
            this.storageFormat = storage.storageFormat;
            this.location = storage.location;
            this.bucketProperty = storage.bucketProperty;
            this.skewed = storage.skewed;
            this.serdeParameters = storage.serdeParameters;
        }

        public Builder setStorageFormat(StorageFormat storageFormat)
        {
            this.storageFormat = storageFormat;
            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setBucketProperty(Optional<HiveBucketProperty> bucketProperty)
        {
            this.bucketProperty = bucketProperty;
            return this;
        }

        public Builder setSkewed(boolean skewed)
        {
            this.skewed = skewed;
            return this;
        }

        public Builder setSerdeParameters(Map<String, String> serdeParameters)
        {
            this.serdeParameters = serdeParameters;
            return this;
        }

        public Storage build()
        {
            return new Storage(storageFormat, location, bucketProperty, skewed, serdeParameters);
        }
    }
}
