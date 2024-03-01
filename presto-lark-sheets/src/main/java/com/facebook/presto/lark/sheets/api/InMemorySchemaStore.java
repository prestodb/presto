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

package com.facebook.presto.lark.sheets.api;

import com.facebook.presto.lark.sheets.LarkSheetsErrorCode;
import com.facebook.presto.spi.PrestoException;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class InMemorySchemaStore
        implements LarkSheetsSchemaStore
{
    private final Map<String, LarkSheetsSchema> schemas = new ConcurrentHashMap<>();

    @Override
    public Optional<LarkSheetsSchema> get(String name)
    {
        return Optional.ofNullable(schemas.get(lower(name)));
    }

    @Override
    public synchronized void insert(LarkSheetsSchema schema)
    {
        String name = lower(schema.getName());
        if (schemas.containsKey(name)) {
            throw new PrestoException(LarkSheetsErrorCode.SCHEMA_ALREADY_EXISTS,
                    format("Schema '%s' already exists or created by others", name));
        }
        schemas.put(name, schema);
    }

    @Override
    public synchronized void delete(String schemaName)
    {
        String name = lower(schemaName);
        if (!schemas.containsKey(name)) {
            throw new PrestoException(LarkSheetsErrorCode.SCHEMA_NOT_EXISTS,
                    format("Schema '%s' does not exist", name));
        }
        schemas.remove(name);
    }

    @Override
    public Iterable<LarkSheetsSchema> listForUser(String user)
    {
        return schemas.values()
                .stream()
                .filter(schema -> schema.isPublicVisible() || user.equalsIgnoreCase(schema.getUser()))
                .collect(toImmutableList());
    }

    private static String lower(String name)
    {
        return name.toLowerCase(Locale.ENGLISH);
    }
}
