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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class NodeUtils
{
    private NodeUtils() {}

    public static List<SortItem> getSortItemsFromOrderBy(Optional<OrderBy> orderBy)
    {
        return orderBy.map(OrderBy::getSortItems).orElse(ImmutableList.of());
    }

    public static Map<String, Expression> mapFromProperties(List<Property> properties)
    {
        return properties.stream().collect(toImmutableMap(
                property -> property.getName().getValue(),
                Property::getValue));
    }
}
