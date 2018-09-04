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
package com.facebook.presto.connector.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Used to let Presto know that a table is bucketed by some set of columns,
 * which changes the functionality of Presto when inserting data through the Thrift connector.
 *
 * bucketCount is currently a default value that gets passed in.
 */
public class ThriftBucketProperty
{
    private final List<String> bucketedBy;
    private final int bucketCount;

    @JsonCreator
    public ThriftBucketProperty(
            @JsonProperty("bucketedBy") List<String> bucketedBy,
            @JsonProperty("bucketCount") int bucketCount)
    {
        this.bucketedBy = requireNonNull(bucketedBy, "bucketedBy is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
    }

    @JsonProperty
    public List<String> getBucketedBy()
    {
        return bucketedBy;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }
}
