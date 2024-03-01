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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.SymbolReference;

class AnySymbol
        extends Symbol
        implements PlanTestSymbol
{
    AnySymbol()
    {
        super("*");
    }

    @Override
    public Symbol toSymbol(SymbolAliases aliases)
    {
        return this;
    }

    @Override
    public SymbolReference toSymbolReference()
    {
        return new AnySymbolReference();
    }

    @Override
    public int hashCode()
    {
        /*
         * It is impossible to implement hashCode in a way that is honors the general equals()/hashCode() contract:
         *
         * 0) If two objects are equal according to the equals(Object) method, then calling the hashCode method on each of the two objects must produce the same integer result.
         * 1) AnySymbol.equals() returns true when the other Object passed to it is of type Symbol.
         * 2) Assume there is an instance of Symbol s1, such that s1.hashCode() returns h1
         * 3) Assume there is an instance of Symbol s2, such that s2.hashCode() returns h2, where h1 != h2
         * 4) Assume there is an instance of AnySymbol, a1
         * 5) By 1, a1.equals(s1) returns true
         * 6) By 1, a1.equals(s1) returns true
         * 7) By 0, 2, a1.hashCode() must return h1
         * 8) By 0, 3, a2.hashCode() must return h2
         * 9) We have arrived at a contradiction -> AnySymbol cannot implement hashCode() in a way that honors the general equals/hashCode contract.
         *
         * In general, overriding hashCode() is unlikely to be useful. Because hashCode() can't be made to work in a way that's consistent with Symbol's hashCode() method, you
         * can't put AnySymbol instances in e.g. a HashSet and do a meaningful comparison to a HashSet of Symbol instances if the implementation of HashSet.equals() relies on the
         * general equals()/hashCode() contract holding.
         *
         * If you find a use case for putting AnySymbol in a hash table, feel free to
         * implement this.
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

        return obj instanceof Symbol;
    }
}
