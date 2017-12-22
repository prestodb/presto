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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public final class QueryActions
{
    private final String setCatalog;
    private final String setSchema;
    private final String setPath;
    private final Map<String, String> setSessionProperties;
    private final Set<String> clearSessionProperties;
    private final Map<String, String> addedPreparedStatements;
    private final Set<String> deallocatedPreparedStatements;
    private final String startedTransactionId;
    private final Boolean clearTransactionId;

    @JsonCreator
    public QueryActions(
            @JsonProperty("setCatalog") String setCatalog,
            @JsonProperty("setSchema") String setSchema,
            @JsonProperty("setPath") String setPath,
            @JsonProperty("setSessionProperties") Map<String, String> setSessionProperties,
            @JsonProperty("clearSessionProperties") Set<String> clearSessionProperties,
            @JsonProperty("addedPreparedStatements") Map<String, String> addedPreparedStatements,
            @JsonProperty("deallocatedPreparedStatements") Set<String> deallocatedPreparedStatements,
            @JsonProperty("startedTransactionId") String startedTransactionId,
            @JsonProperty("clearTransactionId") Boolean clearTransactionId)
    {
        this.setCatalog = setCatalog;
        this.setSchema = setSchema;
        this.setPath = setPath;
        this.setSessionProperties = setSessionProperties != null ? ImmutableMap.copyOf(setSessionProperties) : null;
        this.clearSessionProperties = clearSessionProperties != null ? ImmutableSet.copyOf(clearSessionProperties) : null;
        this.addedPreparedStatements = addedPreparedStatements != null ? ImmutableMap.copyOf(addedPreparedStatements) : null;
        this.deallocatedPreparedStatements = deallocatedPreparedStatements != null ? ImmutableSet.copyOf(deallocatedPreparedStatements) : null;
        this.startedTransactionId = startedTransactionId;
        this.clearTransactionId = clearTransactionId;
    }

    @Nullable
    @JsonProperty
    public String getSetCatalog()
    {
        return setCatalog;
    }

    @Nullable
    @JsonProperty
    public String getSetSchema()
    {
        return setSchema;
    }

    @Nullable
    @JsonProperty
    public String getPath()
    {
        return setPath;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    @Nullable
    @JsonProperty
    public Set<String> getClearSessionProperties()
    {
        return clearSessionProperties;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    @Nullable
    @JsonProperty
    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    @Nullable
    @JsonProperty
    public String getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Nullable
    @JsonProperty
    public Boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("setCatalog", setCatalog)
                .add("setSchema", setSchema)
                .add("setPath", setPath)
                .add("setSessionProperties", setSessionProperties)
                .add("clearSessionProperties", clearSessionProperties)
                .add("addedPreparedStatements", addedPreparedStatements)
                .add("deallocatedPreparedStatements", deallocatedPreparedStatements)
                .add("startedTransactionId", startedTransactionId)
                .add("clearTransactionId", clearTransactionId)
                .omitNullValues()
                .toString();
    }

    @Nullable
    public static QueryActions createIfNecessary(
            Optional<String> setCatalog,
            Optional<String> setSchema,
            Optional<String> setPath,
            Map<String, String> setSessionProperties,
            Set<String> clearSessionProperties,
            Map<String, String> addedPreparedStatements,
            Set<String> deallocatedPreparedStatements,
            Optional<String> startedTransactionId,
            boolean clearTransactionId)
    {
        if (!setCatalog.isPresent() &&
                !setSchema.isPresent() &&
                !setPath.isPresent() &&
                setSessionProperties.isEmpty() &&
                clearSessionProperties.isEmpty() &&
                addedPreparedStatements.isEmpty() &&
                deallocatedPreparedStatements.isEmpty() &&
                !startedTransactionId.isPresent() &&
                !clearTransactionId) {
            // query actions would be empty
            return null;
        }

        return new QueryActions(
                setCatalog.orElse(null),
                setSchema.orElse(null),
                setPath.orElse(null),
                setSessionProperties.isEmpty() ? null : setSessionProperties,
                clearSessionProperties.isEmpty() ? null : clearSessionProperties,
                addedPreparedStatements.isEmpty() ? null : addedPreparedStatements,
                deallocatedPreparedStatements.isEmpty() ? null : deallocatedPreparedStatements,
                startedTransactionId.orElse(null),
                clearTransactionId ? true : null);
    }
}
