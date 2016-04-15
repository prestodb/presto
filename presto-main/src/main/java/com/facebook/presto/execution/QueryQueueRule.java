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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class QueryQueueRule
        implements ResourceGroupSelector
{
    @Nullable
    private final Set<String> userNames;
    private final Map<String, Pattern> sessionPropertyRegexes;

    //Repurposed to represent queueName
    @Nullable
    private final QueryQueueDefinition queryQueueDefinition;
    QueryQueueRule(QueryQueueDefinition queryQueueDefinition, @Nullable Set<String> userNames, Map<String, Pattern> sessionPropertyRegexes)
    {
        this.queryQueueDefinition = requireNonNull(queryQueueDefinition, "queue keys is null");
        this.userNames = ImmutableSet.copyOf(userNames);
        this.sessionPropertyRegexes = ImmutableMap.copyOf(requireNonNull(sessionPropertyRegexes, "sessionPropertyRegexes is null"));
    }

    public static QueryQueueRule createRule(QueryQueueDefinition queryQueueDefinition, @Nullable Set<String> userNames, Map<String, Pattern> sessionPropertyRegexes)
    {
        //propose to remove this method
        return new QueryQueueRule(queryQueueDefinition, userNames, sessionPropertyRegexes);
    }

    @Override
    public Optional<QueryQueueDefinition> match(Statement statement, SessionRepresentation session)
    {
        for (Map.Entry<String, Pattern> entry : sessionPropertyRegexes.entrySet()) {
            String value = session.getSystemProperties().getOrDefault(entry.getKey(), "");
            if (!entry.getValue().matcher(value).matches()) {
                return Optional.empty();
            }
        }

        if (queryQueueDefinition.getIsPublic() && session.getSource().orElse(queryQueueDefinition.getName()).equals(queryQueueDefinition.getName())) {
            return Optional.of(queryQueueDefinition);
        }

        if (!userNames.contains(session.getUser())) {
            return Optional.empty();
        }

        Optional<String> source = session.getSource();
        if (source.isPresent() && source.get() == queryQueueDefinition.getName()) {
                return Optional.of(queryQueueDefinition);
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public QueryQueueDefinition getQueryQueueDefinition()
    {
        return queryQueueDefinition;
    }
}
