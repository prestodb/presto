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

package com.facebook.presto.spi.analyzer;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class UpdateInfo
{
    /**
     * The type of update being performed (e.g., INSERT, DELETE, UPDATE).
     */
    private final String updateType;

    /**
     * The object being updated (e.g., table name, view name).
     */
    private final String updateObject;

    @JsonCreator
    @ThriftConstructor
    public UpdateInfo(
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateObject") String updateObject)
    {
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.updateObject = updateObject;
    }

    @JsonProperty
    @ThriftField(1)
    public String getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    @ThriftField(2)
    public String getUpdateObject()
    {
        return updateObject;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdateInfo that = (UpdateInfo) o;
        return Objects.equals(updateType, that.updateType) && Objects.equals(updateObject, that.updateObject);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(updateType, updateObject);
    }

    @Override
    public String toString()
    {
        return "UpdateInfo{" +
                "updateType='" + updateType + '\'' +
                ", updateObject='" + updateObject + '\'' +
                '}';
    }
}
