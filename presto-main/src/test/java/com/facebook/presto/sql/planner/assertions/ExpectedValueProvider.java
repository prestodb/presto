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

/**
 * An implementation of ExpectedValueProvider<T> should hold the values and
 * SymbolAliases needed to call T's constructor, and call
 * SymbolAlias.toSymbol() to get actual Symbols to pass to T's constructor.
 * Doing this ensures that changes to T's .equals() method that requires a
 * change to T's constructor result in a compilation error.
 *
 * In particular, if adding a new field to T's .equals() method requires
 * passing a value for that field to T's constuctor, using an
 * ExpectedValueProvider that calls T's constructor will ensure that there is
 * a compilation error to be fixed. By contrast, implementing the comparison logic
 * in the test code and using SymbolAliases directly will likely cause tests to
 * pass erroneously without any notification.
 */
public interface ExpectedValueProvider<T>
{
    T getExpectedValue(SymbolAliases aliases);
}
