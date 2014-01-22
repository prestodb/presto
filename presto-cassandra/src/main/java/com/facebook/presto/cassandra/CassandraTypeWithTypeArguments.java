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
package com.facebook.presto.cassandra;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraTypeWithTypeArguments
        implements FullCassandraType
{
    private final CassandraType cassandraType;
    private final List<CassandraType> typeArguments;

    public CassandraTypeWithTypeArguments(CassandraType cassandraType, List<CassandraType> typeArguments)
    {
        this.cassandraType = checkNotNull(cassandraType, "cassandraType is null");
        this.typeArguments = checkNotNull(typeArguments, "typeArguments is null");
    }

    public CassandraType getCassandraType()
    {
        return cassandraType;
    }

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
