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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;

import static java.util.Objects.requireNonNull;

public class PinotExpression
{
    private final String definition;
    private final Origin origin;

    PinotExpression(String definition, Origin origin)
    {
        this.definition = requireNonNull(definition, "definition is null");
        this.origin = requireNonNull(origin, "origin is null");
    }

    static PinotExpression derived(String definition)
    {
        return new PinotExpression(definition, Origin.DERIVED);
    }

    public String getDefinition()
    {
        return definition;
    }

    public Origin getOrigin()
    {
        return origin;
    }
}
