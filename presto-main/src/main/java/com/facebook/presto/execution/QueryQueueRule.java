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
package com.facebook.presto.execution;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryQueueRule
        implements ResourceGroupSelector
{
    @Nullable
    private final Set<String> userNames;

    //Repurposed to represent queueName
    @Nullable
    private final QueryQueueDefinition queryQueueDefinition;
    QueryQueueRule(QueryQueueDefinition queryQueueDefinition, @Nullable Set<String> userNames)
    {
        requireNonNull(queryQueueDefinition, "queue keys is null");
        this.queryQueueDefinition = queryQueueDefinition;
        this.userNames = ImmutableSet.copyOf(userNames);
    }

    public static QueryQueueRule createRule(QueryQueueDefinition queryQueueDefinition, @Nullable Set<String> userNames)
    {
        //propose to remove this method
        return new QueryQueueRule(queryQueueDefinition, userNames);
    }

    @Override
    public Optional<List<QueryQueueDefinition>> match(Statement statement, SessionRepresentation session)
    {
        if (queryQueueDefinition.getIsPublic() && session.getSource().orElse(queryQueueDefinition.getName()) == queryQueueDefinition.getName()) {
            return Optional.of(ImmutableList.of(queryQueueDefinition));
        }
        if (!userNames.contains(session.getUser()) ) {
            return Optional.empty();
        }

        Optional<String> source = session.getSource();
        if (source.isPresent() && source.get() == queryQueueDefinition.getName()) {
                return Optional.of(ImmutableList.of(queryQueueDefinition));
        } else {
            return Optional.empty();
        }
    }
}
