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

import com.facebook.presto.sql.tree.Statement;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class StaticSelector
        implements ResourceGroupSelector
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final ResourceGroupIdTemplate group;

    public StaticSelector(Optional<Pattern> userRegex, Optional<Pattern> sourceRegex, ResourceGroupIdTemplate group)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        this.group = requireNonNull(group, "group is null");
    }

    @Override
    public Optional<ResourceGroupId> match(Statement statement, SelectionContext context)
    {
        if (userRegex.isPresent() && !userRegex.get().matcher(context.getUser()).matches()) {
            return Optional.empty();
        }
        if (sourceRegex.isPresent()) {
            String source = context.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return Optional.empty();
            }
        }

        return Optional.of(group.expandTemplate(context));
    }
}
