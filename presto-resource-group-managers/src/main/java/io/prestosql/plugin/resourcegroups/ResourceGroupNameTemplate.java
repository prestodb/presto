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
package io.prestosql.plugin.resourcegroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class ResourceGroupNameTemplate
{
    public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z][a-zA-Z0-9]*)\\}");

    private final List<NameFragment> fragments;

    @JsonCreator
    public ResourceGroupNameTemplate(String name)
    {
        checkArgument(!name.isEmpty(), "Resource group name is empty");
        checkArgument(name.indexOf('.') < 0, "Invalid resource group name. '%s' contains a '.'", name);
        String nameNoVariables = VARIABLE_PATTERN.matcher(name).replaceAll("");
        checkArgument(
                !(nameNoVariables.contains("{") || nameNoVariables.contains("}") || nameNoVariables.contains("$")),
                "Invalid resource group name. '%s' contains extraneous '$', '{', or '}", name);

        ImmutableList.Builder<NameFragment> builder = ImmutableList.builder();
        Matcher matcher = VARIABLE_PATTERN.matcher(name);
        int lastMatchEnd = 0;
        while (matcher.find()) {
            int matchBegin = matcher.start();
            int matchEnd = matcher.end();
            if (matchBegin > lastMatchEnd) {
                builder.add(NameFragment.literal(name.substring(lastMatchEnd, matchBegin)));
            }
            builder.add(NameFragment.variable(name.substring(matchBegin + 2, matchEnd - 1)));
            lastMatchEnd = matchEnd;
        }

        if (lastMatchEnd != name.length()) {
            builder.add(NameFragment.literal(name.substring(lastMatchEnd, name.length())));
        }

        this.fragments = builder.build();
    }

    public Set<String> getVariableNames()
    {
        return fragments.stream()
                .filter(NameFragment::isVariable)
                .map(NameFragment::getVariable)
                .collect(toImmutableSet());
    }

    public String expandTemplate(VariableMap variables)
    {
        StringBuilder builder = new StringBuilder();
        for (NameFragment fragment : fragments) {
            if (fragment.isVariable()) {
                String value = variables.getValue(fragment.getVariable());
                checkArgument(value != null, "unresolved variable '%s' in resource group '%s', available: %s", fragment.getVariable(), this, variables);
                builder.append(value);
            }
            else {
                builder.append(fragment.getLiteral());
            }
        }

        return builder.toString();
    }

    @Override
    public String toString()
    {
        return Joiner.on("").join(fragments);
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
        return Objects.equals(fragments, that.fragments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fragments);
    }

    private static final class NameFragment
    {
        private final String variable;
        private final String literal;

        private NameFragment(String variable, String literal)
        {
            checkArgument((variable == null) ^ (literal == null), "only one of variable or literal can be defined");
            this.variable = variable;
            this.literal = literal;
        }

        public static NameFragment variable(String variable)
        {
            return new NameFragment(variable, null);
        }

        public static NameFragment literal(String literal)
        {
            return new NameFragment(null, literal);
        }

        public boolean isVariable()
        {
            return variable != null;
        }

        public String getVariable()
        {
            return variable;
        }

        public String getLiteral()
        {
            return literal;
        }

        @Override
        public String toString()
        {
            if (variable != null) {
                return "${" + variable + "}";
            }

            return literal;
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
            NameFragment that = (NameFragment) o;
            return Objects.equals(variable, that.variable) &&
                    Objects.equals(literal, that.literal);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(variable, literal);
        }
    }
}
