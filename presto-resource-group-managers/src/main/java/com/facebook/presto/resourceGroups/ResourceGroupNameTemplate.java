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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ResourceGroupNameTemplate
{
    private static final Pattern USER_PATTERN = Pattern.compile(Pattern.quote("${USER}"));
    private static final Pattern SOURCE_PATTERN = Pattern.compile(Pattern.quote("${SOURCE}"));
    private static final Pattern PARAMETER_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

    private final String name;

    @JsonCreator
    public ResourceGroupNameTemplate(String name)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "Resource group name is empty");
        checkArgument(name.indexOf('.') < 0, "Invalid resource group name. '%s' contains a '.'", name);
        Matcher matcher = PARAMETER_PATTERN.matcher(name);
        while (matcher.find()) {
            String group = matcher.group(1);
            checkArgument(group.equals("USER") || group.equals("SOURCE"), "Unsupported template parameter: ${%s}", group);
        }
    }

    public String expandTemplate(SelectionContext context)
    {
        String expanded = USER_PATTERN.matcher(name).replaceAll(context.getUser());
        return SOURCE_PATTERN.matcher(expanded).replaceAll(context.getSource().orElse(""));
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceGroupNameTemplate that = (ResourceGroupNameTemplate) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
