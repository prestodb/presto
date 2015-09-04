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

import com.facebook.presto.Session;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryQueueDefinition
{
    private static final Pattern USER_PATTERN = Pattern.compile(Pattern.quote("${USER}"));
    private static final Pattern SOURCE_PATTERN = Pattern.compile(Pattern.quote("${SOURCE}"));
    private final String template;
    private final int maxConcurrent;
    private final int maxQueued;

    public QueryQueueDefinition(String template, int maxConcurrent, int maxQueued)
    {
        this.template = requireNonNull(template, "template is null");
        Matcher matcher = Pattern.compile("\\$\\{(.*?)\\}").matcher(template);
        while (matcher.find()) {
            String group = matcher.group(1);
            checkArgument(group.equals("USER") || group.equals("SOURCE"), "Unsupported template parameter: ${%s}", group);
        }
        checkArgument(maxConcurrent > 0, "maxConcurrent must be positive");
        checkArgument(maxQueued > 0, "maxQueued must be positive");
        this.maxConcurrent = maxConcurrent;
        this.maxQueued = maxQueued;
    }

    public String getExpandedTemplate(Session session)
    {
        String expanded = USER_PATTERN.matcher(template).replaceAll(session.getUser());
        return SOURCE_PATTERN.matcher(expanded).replaceAll(session.getSource().orElse(""));
    }

    String getTemplate()
    {
        return template;
    }

    public int getMaxConcurrent()
    {
        return maxConcurrent;
    }

    public int getMaxQueued()
    {
        return maxQueued;
    }
}
