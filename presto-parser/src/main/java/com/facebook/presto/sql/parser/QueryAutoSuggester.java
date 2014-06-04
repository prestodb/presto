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

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.TokenStream;
import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

public class QueryAutoSuggester
{
    public static final int MINLENGTH = 4;
    public static final int PRECISION = 2;
    private String query;
    private String[] commands;

    public QueryAutoSuggester(String query)
    {
        this.query = query;
        this.commands = getCommands();
    }

    private String[] getCommands()
    {
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream());
        StatementLexer lexer = new StatementLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        StatementParser parser = new StatementParser(tokenStream);
        return parser.getTokenNames();
    }

    public String getSuggestion()
    {
        ArrayList<String> suggestions = new ArrayList<String>();
        String[] queryTokens = query.split(" ");
        for (String queryToken : queryTokens) {
            List<String> matchingCommands = getMatchingCommands(queryToken);
            if (matchingCommands.size() > 0) {
                suggestions.add(matchingCommands.get(0));
            }
            else {
                suggestions.add(queryToken);
            }
        }
        return Joiner.on(" ").join(suggestions);
    }

    private List<String> getMatchingCommands(final String queryToken)
    {
        ArrayList<String> matches = new ArrayList<String>();
        for (String command : commands) {
            if (queryToken.length() >= MINLENGTH && computeDistance(queryToken, command) <= PRECISION) {
                matches.add(command);
            }
        }
        Collections.sort(matches,  new Comparator<String>()
            {
                public int compare(String string1, String string2)
                {
                    return computeDistance(queryToken, string1) - computeDistance(queryToken, string2);
                }
            }
        );
        return matches;
    }

    // Calculate Levenshtein distance
    private static int computeDistance(String s1, String s2)
    {
    /**
    * Returns the edit distance needed to convert string s1 to s2
    * If returns 0, the strings are same
    * If returns 1, that means either a character is added, removed or replaced
    */
    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();
    int[] costs = new int[s2.length() + 1];
    for (int i = 0; i <= s1.length(); i++) {
        int lastValue = i;
        for (int j = 0; j <= s2.length(); j++) {
            if (i == 0) {
                costs[j] = j;
            }
            else {
                if (j > 0) {
                    int newValue = costs[j - 1];
                    if (s1.charAt(i - 1) != s2.charAt(j - 1)) {
                        newValue = Math.min(Math.min(newValue, lastValue), costs[j]) + 1;
                    }
                    costs[j - 1] = lastValue;
                    lastValue = newValue;
                }
            }
        }
        if (i > 0) {
            costs[s2.length()] = lastValue;
        }
    }
    return costs[s2.length()];
    }
}
