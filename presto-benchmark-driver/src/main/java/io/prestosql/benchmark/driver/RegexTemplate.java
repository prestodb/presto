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
package io.prestosql.benchmark.driver;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class RegexTemplate
{
    private static final Method NAMED_GROUPS_METHOD;

    static {
        try {
            NAMED_GROUPS_METHOD = Pattern.class.getDeclaredMethod("namedGroups");
            NAMED_GROUPS_METHOD.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private final String template;
    private final Pattern pattern;
    private final List<String> fieldNames;

    public RegexTemplate(String template)
    {
        this.template = requireNonNull(template, "template is null");

        try {
            this.pattern = Pattern.compile(template);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid template: " + template, e);
        }

        Map<String, Integer> namedGroups;
        try {
            namedGroups = (Map<String, Integer>) NAMED_GROUPS_METHOD.invoke(pattern);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        ImmutableSortedMap<Integer, String> sortedGroups = ImmutableSortedMap.copyOf(ImmutableBiMap.copyOf(namedGroups).inverse());
        this.fieldNames = ImmutableList.copyOf(sortedGroups.values());
    }

    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    public Optional<Map<String, String>> parse(String value)
    {
        requireNonNull(value, "value is null");

        Matcher matcher = pattern.matcher(value);
        if (!matcher.matches()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<String, String> fieldsBuilder = ImmutableMap.builder();
        for (int index = 0; index < fieldNames.size(); index++) {
            String fieldName = fieldNames.get(index);
            String fieldValue = matcher.group(index + 1);
            if (fieldValue != null) {
                fieldsBuilder.put(fieldName, fieldValue);
            }
        }

        Map<String, String> fields = fieldsBuilder.build();
        return Optional.of(fields);
    }

    @Override
    public String toString()
    {
        return template;
    }
}
