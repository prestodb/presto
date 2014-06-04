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
package com.facebook.presto.sql.parser;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestQueryAutoSuggester
{
    @Test
    public void testSuggesterCorrectInput()
    {
        QueryAutoSuggester queryAutoSuggester = new QueryAutoSuggester(" select * FROM foo ");
        assertEquals(queryAutoSuggester.getSuggestion(), " SELECT * FROM foo");
    }

    @Test
    public void testSuggesterIncomplete()
    {
        QueryAutoSuggester queryAutoSuggester = new QueryAutoSuggester(" selec * FROM foo ");
        assertEquals(queryAutoSuggester.getSuggestion(), " SELECT * FROM foo");
    }

    @Test
    public void testSuggesterEmptyInput()
    {
        QueryAutoSuggester queryAutoSuggester = new QueryAutoSuggester("");
        assertEquals(queryAutoSuggester.getSuggestion(), "");
    }
}
