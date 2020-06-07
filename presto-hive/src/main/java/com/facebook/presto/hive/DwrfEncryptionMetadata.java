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
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionMetadata
        implements EncryptionMetadata
{
    private final Map<String, byte[]> fieldToKeyData;

    @JsonCreator
    public DwrfEncryptionMetadata(
            @JsonProperty Map<String, byte[]> fieldToKeyData)
    {
        this.fieldToKeyData = ImmutableMap.copyOf(requireNonNull(fieldToKeyData, "fieldToKeyData is null"));
    }

    @JsonProperty
    public Map<String, byte[]> getFieldToKeyData()
    {
        return fieldToKeyData;
    }

    @Override
    public int hashCode()
    {
        return hash(fieldToKeyData);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }

        DwrfEncryptionMetadata otherObj = (DwrfEncryptionMetadata) obj;
        return compareFieldMap(this.fieldToKeyData, otherObj.fieldToKeyData);
    }

    private static boolean compareFieldMap(Map<String, byte[]> obj1, Map<String, byte[]> obj2)
    {
        if (obj1 == null && obj2 == null) {
            return true;
        }

        if (obj1 == null || obj2 == null) {
            return false;
        }

        if (obj1.size() != obj2.size()) {
            return false;
        }

        return obj1.entrySet().stream().allMatch(entry -> Arrays.equals(entry.getValue(), obj2.get(entry.getKey())));
    }
}
