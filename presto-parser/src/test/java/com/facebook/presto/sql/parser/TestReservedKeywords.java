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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

/**
 * Guards the words that Presto accepts as unquoted identifiers.
 * <p>
 * Making a keyword reserved is a backwards incompatible change: every existing query that uses the
 * word as a column, table or alias name stops parsing. It is easy to introduce by accident, because
 * adding a keyword token for a new piece of syntax makes that word reserved as a side effect, and
 * nothing else in the parser tests notices.
 * <p>
 * Two lists are checked in, and each is parsed in every position where an identifier is allowed:
 * <ul>
 * <li>the words Presto already accepts, so a change cannot take one away;</li>
 * <li>the words the SQL standard says are not reserved and Presto does not use yet, so a change
 * cannot claim one for new syntax without noticing.</li>
 * </ul>
 * Both lists are checked in rather than derived from the grammar. Deriving them would drop a word
 * from the test in the same change that starts treating it as a keyword, which is exactly when the
 * test needs to fail.
 */
public class TestReservedKeywords
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().build();

    private static final String PRESTO_NON_RESERVED_WORDS_RESOURCE = "presto-non-reserved-words.txt";
    private static final String SQL_STANDARD_NON_RESERVED_WORDS_RESOURCE = "sql-2016-non-reserved-words.txt";

    /**
     * Positions in which a non-reserved word must be usable as an identifier. Each template has every
     * occurrence of {@code %s} replaced with the word.
     */
    private static final List<String> IDENTIFIER_POSITIONS = ImmutableList.of(
            "SELECT %s FROM t",
            "SELECT 1 AS %s FROM t",
            "SELECT t.%s FROM t",
            "SELECT * FROM %s",
            "SELECT * FROM t AS %s",
            "SELECT * FROM %s.%s.%s",
            "SELECT * FROM t WHERE %s = 1",
            "SELECT * FROM t GROUP BY %s",
            "SELECT * FROM t ORDER BY %s",
            "SELECT count(*) OVER (PARTITION BY %s) FROM t",
            "WITH %s AS (SELECT 1) SELECT * FROM %s",
            "CREATE TABLE t (%s bigint)");

    /**
     * Every word Presto accepts as an unquoted identifier today must keep working.
     * <p>
     * This is the {@code nonReserved} rule of the grammar plus words that were reserved by mistake in
     * the past and reverted, notably {@code TRIM}. A change that removes a word from
     * {@code nonReserved}, or adds a keyword token for one of the reverted words, fails here.
     * <p>
     * The statements only have to parse. This module has no analyzer or query runner, so no table
     * named {@code t} is resolved and no table is created.
     */
    @Test
    public void testCurrentNonReservedWords()
    {
        Set<String> words = prestoNonReservedWords();

        List<String> messages = identifierPositionFailures(words);
        if (!messages.isEmpty()) {
            fail(format(
                    "%s word/position combinations no longer parse as identifiers. Presto accepts these words as " +
                            "unquoted identifiers today, so reserving one breaks every existing query that uses it " +
                            "as a column, table or alias name. Keep the word in the nonReserved rule of " +
                            "SqlBase.g4.\n%s",
                    messages.size(),
                    String.join("\n", messages)));
        }
    }

    /**
     * Every word the SQL standard lists as non-reserved and Presto does not use as a keyword yet must
     * be usable as an identifier.
     * <p>
     * These words are ordinary identifiers today precisely because no syntax has claimed them. Syntax
     * added later tends to reach for exactly them (the JSON and polymorphic-table-function vocabulary
     * in particular), and a keyword token for one makes it reserved as a side effect. Failing here is
     * the prompt to put the new keyword in the {@code nonReserved} rule of the grammar instead.
     * <p>
     * Words Presto already treats as non-reserved are subtracted, because
     * {@link #testCurrentNonReservedWords()} covers those. The subtraction is between two checked-in
     * lists, so it does not change when the grammar does.
     */
    @Test
    public void testFutureProof()
    {
        Set<String> standardWords = readWordListResource(SQL_STANDARD_NON_RESERVED_WORDS_RESOURCE);
        assertFalse(standardWords.isEmpty(), "SQL standard non-reserved word list is empty");

        Set<String> words = Sets.difference(standardWords, prestoNonReservedWords());
        assertFalse(words.isEmpty(), "no SQL standard non-reserved words are left to guard");

        List<String> messages = identifierPositionFailures(words);
        if (!messages.isEmpty()) {
            fail(format(
                    "%s word/position combinations no longer parse as identifiers. These words are non-reserved in " +
                            "the SQL standard (ISO/IEC 9075-2:2016, Subclause 5.2), so reserving them breaks existing " +
                            "queries and diverges from the standard at the same time. If new syntax needs one of " +
                            "them, add it to the nonReserved rule in SqlBase.g4 and to %s.\n%s",
                    messages.size(),
                    PRESTO_NON_RESERVED_WORDS_RESOURCE,
                    String.join("\n", messages)));
        }
    }

    private static Set<String> prestoNonReservedWords()
    {
        Set<String> words = readWordListResource(PRESTO_NON_RESERVED_WORDS_RESOURCE);
        assertFalse(words.isEmpty(), "Presto non-reserved word list is empty");
        return words;
    }

    /**
     * Parses each word in every position where an identifier is allowed, returning one message per
     * failing combination so that a failure names the offending word and position.
     */
    private static List<String> identifierPositionFailures(Set<String> words)
    {
        ImmutableList.Builder<String> failures = ImmutableList.builder();
        for (String word : words) {
            for (String template : IDENTIFIER_POSITIONS) {
                // lower case, because that is how these words appear in real queries
                String sql = template.replace("%s", word.toLowerCase(Locale.ENGLISH));
                try {
                    SQL_PARSER.createStatement(sql, PARSING_OPTIONS);
                }
                catch (ParsingException e) {
                    failures.add(format("%s: %s [%s]", word, sql, e.getErrorMessage()));
                }
            }
        }
        return failures.build();
    }

    /**
     * Reads a word-per-line resource, ignoring blank lines and {@code #} comments.
     */
    private static Set<String> readWordListResource(String resource)
    {
        String contents;
        try {
            contents = Resources.toString(Resources.getResource(resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Arrays.stream(contents.split("\n"))
                .map(String::trim)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .collect(toImmutableSet());
    }
}
