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
package io.prestosql.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.ListIndexesIterable;
import io.prestosql.spi.block.SortOrder;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MongoIndex
{
    private final String name;
    private final List<MongodbIndexKey> keys;
    private final boolean unique;

    public static List<MongoIndex> parse(ListIndexesIterable<Document> indexes)
    {
        ImmutableList.Builder<MongoIndex> builder = ImmutableList.builder();
        for (Document index : indexes) {
            // TODO: v, ns, sparse fields
            Document key = (Document) index.get("key");
            String name = index.getString("name");
            boolean unique = index.getBoolean("unique", false);

            if (key.containsKey("_fts")) { // Full Text Search
                continue;
            }
            builder.add(new MongoIndex(name, parseKey(key), unique));
        }

        return builder.build();
    }

    private static List<MongodbIndexKey> parseKey(Document key)
    {
        ImmutableList.Builder<MongodbIndexKey> builder = ImmutableList.builder();

        for (String name : key.keySet()) {
            Object value = key.get(name);
            if (value instanceof Number) {
                int order = ((Number) value).intValue();
                checkState(order == 1 || order == -1, "Unknown index sort order");
                builder.add(new MongodbIndexKey(name, order == 1 ? SortOrder.ASC_NULLS_LAST : SortOrder.DESC_NULLS_LAST));
            }
            else if (value instanceof String) {
                builder.add(new MongodbIndexKey(name, (String) value));
            }
            else {
                throw new UnsupportedOperationException("Unknown index type: " + value.toString());
            }
        }

        return builder.build();
    }

    public MongoIndex(String name, List<MongodbIndexKey> keys, boolean unique)
    {
        this.name = name;
        this.keys = keys;
        this.unique = unique;
    }

    public String getName()
    {
        return name;
    }

    public List<MongodbIndexKey> getKeys()
    {
        return keys;
    }

    public boolean isUnique()
    {
        return unique;
    }

    public static class MongodbIndexKey
    {
        private final String name;
        private final Optional<SortOrder> sortOrder;
        private final Optional<String> type;

        public MongodbIndexKey(String name, SortOrder sortOrder)
        {
            this(name, Optional.of(sortOrder), Optional.empty());
        }

        public MongodbIndexKey(String name, String type)
        {
            this(name, Optional.empty(), Optional.of(type));
        }

        public MongodbIndexKey(String name, Optional<SortOrder> sortOrder, Optional<String> type)
        {
            this.name = requireNonNull(name, "name is null");
            this.sortOrder = sortOrder;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public Optional<SortOrder> getSortOrder()
        {
            return sortOrder;
        }

        public Optional<String> getType()
        {
            return type;
        }
    }
}
