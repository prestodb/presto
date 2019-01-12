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
package io.prestosql.plugin.cassandra;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CassandraTypeWithTypeArguments
        implements FullCassandraType
{
    private final CassandraType cassandraType;
    private final List<CassandraType> typeArguments;

    public CassandraTypeWithTypeArguments(CassandraType cassandraType, List<CassandraType> typeArguments)
    {
        this.cassandraType = requireNonNull(cassandraType, "cassandraType is null");
        this.typeArguments = requireNonNull(typeArguments, "typeArguments is null");
    }

    @Override
    public CassandraType getCassandraType()
    {
        return cassandraType;
    }

    @Override
    public List<CassandraType> getTypeArguments()
    {
        return typeArguments;
    }

    @Override
    public String toString()
    {
        if (typeArguments != null) {
            return cassandraType.toString() + typeArguments.toString();
        }
        else {
            return cassandraType.toString();
        }
    }
}
