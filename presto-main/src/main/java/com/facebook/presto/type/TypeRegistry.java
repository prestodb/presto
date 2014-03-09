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
package com.facebook.presto.type;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class TypeRegistry
        implements TypeManager
{
    private final ConcurrentMap<String, Type> types = new ConcurrentHashMap<>();

    public TypeRegistry()
    {
        this(ImmutableSet.<Type>of());
    }

    @Inject
    public TypeRegistry(Set<Type> types)
    {
        checkNotNull(types, "types is null");

        // always add the built-in types; Presto will not function without these
        addType(BooleanType.BOOLEAN);
        addType(BigintType.BIGINT);
        addType(DoubleType.DOUBLE);
        addType(VarcharType.VARCHAR);

        for (Type type : types) {
            addType(type);
        }
    }

    @Override
    public Type getType(String typeName)
    {
        return types.get(typeName.toLowerCase());
    }

    public void addType(Type type)
    {
        checkNotNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getName().toLowerCase(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type.getName());
    }
}
