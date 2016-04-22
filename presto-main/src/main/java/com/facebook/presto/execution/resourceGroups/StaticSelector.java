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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class StaticSelector
        implements ResourceGroupSelector
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final Map<String, Pattern> sessionPropertyRegexes;
    private final ResourceGroupIdTemplate group;

    public StaticSelector(Optional<Pattern> userRegex, Optional<Pattern> sourceRegex, Map<String, Pattern> sessionPropertyRegexes, ResourceGroupIdTemplate group)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        this.sessionPropertyRegexes = ImmutableMap.copyOf(requireNonNull(sessionPropertyRegexes, "sessionPropertyRegexes is null"));
        this.group = requireNonNull(group, "group is null");
    }

    @Override
    public Optional<ResourceGroupId> match(Statement statement, SessionRepresentation session)
    {
        if (userRegex.isPresent() && !userRegex.get().matcher(session.getUser()).matches()) {
            return Optional.empty();
        }
        if (sourceRegex.isPresent()) {
            String source = session.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return Optional.empty();
            }
        }

        for (Map.Entry<String, Pattern> entry : sessionPropertyRegexes.entrySet()) {
            String value = session.getSystemProperties().getOrDefault(entry.getKey(), "");
            if (!entry.getValue().matcher(value).matches()) {
                return Optional.empty();
            }
        }

        return Optional.of(group.expandTemplate(session));
    }
}
