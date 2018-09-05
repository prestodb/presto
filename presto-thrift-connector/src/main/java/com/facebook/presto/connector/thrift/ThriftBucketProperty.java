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

import java.util.List;

public class ThriftBucketProperty
{
    private final List<String> bucketedBy;
    private final int bucketCount;

    public ThriftBucketProperty(List<String> bucketedBy, int bucketCount)
    {
        this.bucketedBy = bucketedBy;
        this.bucketCount = bucketCount;
    }

    public List<String> getBucketedBy()
    {
        return bucketedBy;
    }

    public int getBucketCount()
    {
        return bucketCount;
    }
}
