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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Collection;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class CanonicalVariableAllocator
        extends VariableAllocator
{
    protected static final Pattern DISALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9_\\-$]+");

    public CanonicalVariableAllocator()
    {
        super();
    }

    public CanonicalVariableAllocator(Collection<VariableReferenceExpression> initial)
    {
        super(initial);
    }

    public VariableReferenceExpression newVariable(Optional<SourceLocation> sourceLocation, String nameHint, Type type, String suffix)
    {
        requireNonNull(nameHint, "name is null");
        requireNonNull(type, "type is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (tryParse(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        String unique = nameHint;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }
        // remove special characters for other special serde
        unique = DISALLOWED_CHAR_PATTERN.matcher(unique).replaceAll("_");

        String attempt = unique;
//        while (variables.putIfAbsent(attempt, type) != null) {
//            attempt = unique + "_" + nextId();
//        }

        return new VariableReferenceExpression(sourceLocation, attempt, type);
    }
}
