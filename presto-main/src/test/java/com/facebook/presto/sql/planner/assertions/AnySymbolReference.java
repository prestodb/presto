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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.tree.SymbolReference;

class AnySymbolReference
        extends SymbolReference
{
    AnySymbolReference()
    {
        super("*");
    }

    @Override
    public int hashCode()
    {
        /*
         * See AnySymbol.hashCode() for an explanation of why this is the way it is.
         * If you find a use case for putting AnySymbolReference in a hash table,
         * feel free to implement this.
         */
        throw new UnsupportedOperationException("Test object");
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        return obj instanceof SymbolReference;
    }
}
