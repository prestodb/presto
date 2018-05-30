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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class QueryId
{
    @JsonCreator
    public static QueryId valueOf(String queryId)
    {
        List<String> ids = parseDottedId(queryId, 1, "queryId");
        return new QueryId(ids.get(0));
    }

    private final String id;

    public QueryId(String id)
    {
        this.id = validateId(id);
    }

    public String getId()
    {
        return id;
    }

    @Override
    @JsonValue
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
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
        QueryId other = (QueryId) obj;
        return Objects.equals(this.id, other.id);
    }

    //
    // Id helper methods
    //

    private static final Pattern ID_PATTERN = Pattern.compile("[_a-z0-9]+");

    public static String validateId(String id)
    {
        requireNonNull(id, "id is null");
        checkArgument(!id.isEmpty(), "id is empty");
        checkArgument(ID_PATTERN.matcher(id).matches(), "Invalid id %s", id);
        return id;
    }

    public static List<String> parseDottedId(String id, int expectedParts, String name)
    {
        requireNonNull(id, "id is null");
        checkArgument(expectedParts > 0, "expectedParts must be at least 1");
        requireNonNull(name, "name is null");

        List<String> ids = unmodifiableList(Arrays.asList(id.split("\\.")));
        checkArgument(ids.size() == expectedParts, "Invalid %s %s", name, id);

        for (String part : ids) {
            checkArgument(!part.isEmpty(), "Invalid id %s", id);
            checkArgument(ID_PATTERN.matcher(part).matches(), "Invalid id %s", id);
        }
        return ids;
    }

    private static void checkArgument(boolean condition, String message, Object... messageArgs)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, messageArgs));
        }
    }
}
