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

package com.facebook.presto.matching;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public abstract class Pattern
{
    private static final Pattern ANY = new MatchByClass(Object.class);

    private Pattern() {}

    public abstract boolean matches(Object object);

    public static Pattern any()
    {
        return ANY;
    }

    public static <T> Pattern matchByClass(Class<T> objectClass)
    {
        return new MatchByClass(objectClass);
    }

    static class MatchByClass<T>
            extends Pattern
    {
        private final Class<T> objectClass;

        MatchByClass(Class<T> objectClass)
        {
            this.objectClass = requireNonNull(objectClass, "objectClass is null");
        }

        Class<T> getObjectClass()
        {
            return objectClass;
        }

        @Override
        public boolean matches(Object object)
        {
            return objectClass.isInstance(object);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("objectClass", objectClass)
                    .toString();
        }
    }
}
