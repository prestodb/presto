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
package io.prestosql.sql.gen;

import java.lang.invoke.MethodType;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class Binding
{
    private final long bindingId;
    private final MethodType type;

    public Binding(long bindingId, MethodType type)
    {
        this.bindingId = bindingId;
        this.type = type;
    }

    public long getBindingId()
    {
        return bindingId;
    }

    public MethodType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bindingId", bindingId)
                .add("type", type)
                .toString();
    }
}
