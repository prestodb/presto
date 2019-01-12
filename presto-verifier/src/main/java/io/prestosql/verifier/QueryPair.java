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
package io.prestosql.verifier;

public class QueryPair
{
    private final String suite;
    private final String name;
    private final Query test;
    private final Query control;

    public QueryPair(String suite, String name, Query test, Query control)
    {
        this.suite = suite;
        this.name = name;
        this.test = test;
        this.control = control;
    }

    public String getSuite()
    {
        return suite;
    }

    public String getName()
    {
        return name;
    }

    public Query getTest()
    {
        return test;
    }

    public Query getControl()
    {
        return control;
    }
}
