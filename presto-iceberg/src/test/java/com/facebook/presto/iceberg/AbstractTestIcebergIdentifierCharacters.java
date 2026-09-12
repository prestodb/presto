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
package com.facebook.presto.iceberg;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static java.lang.String.format;

/**
 * Identifier character coverage around which characters Presto accepts
 * in Iceberg schema and table names
 */
public abstract class AbstractTestIcebergIdentifierCharacters
        extends AbstractTestQueryFramework
{
    /**
     * Creates a schema and a table under the given identifiers, round-trips a row through them,
     * confirms both are discoverable by their stored names, then drops them.
     */
    protected void assertIdentifiersUsable(String schemaIdentifier, String tableIdentifier, String storedSchema, String storedTable)
    {
        String qualified = format("%s.%s", schemaIdentifier, tableIdentifier);
        assertUpdate(format("CREATE SCHEMA %s", schemaIdentifier));
        try {
            assertUpdate(format("CREATE TABLE %s (id integer, v varchar)", qualified));
            try {
                assertUpdate(format("INSERT INTO %s VALUES (1, 'a')", qualified), 1);
                assertQuery(format("SELECT id, v FROM %s", qualified), "VALUES (1, 'a')");
                // The name has to survive into the metadata the catalog reports back, not just the write path.
                assertQuery(
                        format("SELECT count(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
                                literal(storedSchema), literal(storedTable)),
                        "VALUES 1");
                assertQuery(
                        format("SELECT count(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'",
                                literal(storedSchema), literal(storedTable)),
                        "VALUES 2");
            }
            finally {
                assertUpdate(format("DROP TABLE %s", qualified));
            }
        }
        finally {
            assertQuerySucceeds(format("DROP SCHEMA %s", schemaIdentifier));
        }
    }

    /** Same as {@link #assertIdentifiersUsable}, for an identifier that is already lower case. */
    protected void assertIdentifiersUsable(String schemaIdentifier, String tableIdentifier)
    {
        assertIdentifiersUsable(schemaIdentifier, tableIdentifier, undelimit(schemaIdentifier), undelimit(tableIdentifier));
    }

    private static String literal(String value)
    {
        return value.replace("'", "''");
    }

    /** Strips the surrounding double quotes and unescapes {@code ""}, giving the name as stored. */
    private static String undelimit(String identifier)
    {
        if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
            return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
        }
        return identifier;
    }

    @Test
    public void testLowercaseLettersDigitsAndUnderscore()
    {
        assertIdentifiersUsable("ident_schema_a1", "ident_table_b2");
    }

    @Test
    public void testUnderscoreLeadingIdentifier()
    {
        assertIdentifiersUsable("_ident_schema", "_ident_table");
    }

    @Test
    public void testIdentifierStartingWithADigitMustBeDelimited()
    {
        assertQueryFails("CREATE SCHEMA 1ident_schema",
                ".*identifiers must not start with a digit; surround the identifier with double quotes.*");
        assertIdentifiersUsable("\"1ident_schema\"", "\"2ident_table\"");
    }

    @Test
    public void testUppercaseIsAcceptedButFoldedToLowercase()
    {
        assertIdentifiersUsable("IDENT_SCHEMA_UPPER", "IDENT_TABLE_UPPER", "ident_schema_upper", "ident_table_upper");
    }

    @Test
    public void testDelimitedUppercaseIsAlsoFolded()
    {
        // Delimiting does not preserve case in Presto, unlike most other engines.
        assertIdentifiersUsable("\"IdentSchemaMixed\"", "\"IdentTableMixed\"", "identschemamixed", "identtablemixed");
    }

    @Test
    public void testUppercaseAndLowercaseSpellingsNameTheSameTable()
    {
        assertUpdate("CREATE SCHEMA ident_folding_schema");
        try {
            assertUpdate("CREATE TABLE ident_folding_schema.ident_folding_table (id integer)");
            try {
                assertUpdate("INSERT INTO IDENT_FOLDING_SCHEMA.IDENT_FOLDING_TABLE VALUES 1", 1);
                assertQuery("SELECT id FROM \"IDENT_FOLDING_SCHEMA\".\"IDENT_FOLDING_TABLE\"", "VALUES 1");
            }
            finally {
                assertUpdate("DROP TABLE ident_folding_schema.ident_folding_table");
            }
        }
        finally {
            assertQuerySucceeds("DROP SCHEMA ident_folding_schema");
        }
    }

    @Test
    public void testDelimitedLowercaseLettersDigitsAndUnderscore()
    {
        assertIdentifiersUsable("\"ident_schema_q9\"", "\"ident_table_q9\"");
    }

    @Test
    public void testDelimitedUnderscoreLeadingIdentifier()
    {
        assertIdentifiersUsable("\"_ident_schema_q\"", "\"_ident_table_q\"");
    }

    @Test
    public void testDelimitedDotInASchemaName()
    {
        assertIdentifiersUsable("\"ident.schema\"", "ident_dot_table");
    }

    @Test
    public void testDelimitedDotInATableName()
    {
        assertIdentifiersUsable("ident_dot_schema", "\"ident.table\"");
    }

    @Test
    public void testDelimitedHyphen()
    {
        assertIdentifiersUsable("\"ident-schema\"", "\"ident-table\"");
    }

    @Test
    public void testDelimitedSpace()
    {
        assertIdentifiersUsable("\"ident schema\"", "\"ident table\"");
    }

    @Test
    public void testDelimitedPercentAndPunctuation()
    {
        assertIdentifiersUsable("\"ident%schema!+~\"", "\"ident%table!+~\"");
    }

    @Test
    public void testDelimitedLeadingSymbolAndDigit()
    {
        assertIdentifiersUsable("\"%1ident schema\"", "\"-2ident table\"");
    }

    @Test
    public void testDelimitedEmbeddedDoubleQuote()
    {
        // "" is the escape for a single double quote inside a delimited identifier.
        assertIdentifiersUsable("\"ident\"\"schema\"", "\"ident\"\"table\"", "ident\"schema", "ident\"table");
    }

    @Test
    public void testDelimitedReservedKeyword()
    {
        assertIdentifiersUsable("\"select\"", "\"table\"");
    }

    @Test
    public void testDelimitedNonLatinLetters()
    {
        // Accented Latin and Cyrillic. Folding uses Locale.ENGLISH, so these lower-case predictably.
        assertIdentifiersUsable("\"schéma_accentué\"", "\"таблица\"");
    }

    @Test
    public void testDelimitedHiraganaSyllables()
    {
        assertIdentifiersUsable("\"ひらがなスキーマ\"", "\"ひらがなテーブル\"");
    }

    @Test
    public void testDelimitedIdeographs()
    {
        assertIdentifiersUsable("\"模式\"", "\"表名\"");
    }

    @Test
    public void testHyphenIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ident-schema", ".*mismatched input '-'.*");
    }

    @Test
    public void testNonLatinLetterIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA 模式", ".*mismatched input '模'.*");
    }

    @Test
    public void testDollarSignIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ident$schema", ".*mismatched input '\\$'.*");
    }

    @Test
    public void testColonIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ident:schema", ".*identifiers must not contain ':'.*");
    }

    @Test
    public void testAtSignIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ident@schema_bare", ".*identifiers must not contain '@'.*");
    }

    @Test
    public void testSpaceIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ident schema", ".*mismatched input 'schema'\\. Expecting: '\\.', 'WITH', <EOF>.*");
    }

    @Test
    public void testPercentIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ident%schema", ".*mismatched input '%'\\. Expecting: '\\.', 'WITH', <EOF>.*");
    }

    @Test
    public void testHiraganaIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA ひらがな", ".*mismatched input 'ひ'\\. Expecting: 'IF', <identifier>.*");
    }

    @Test
    public void testReservedKeywordIsRejectedUnquoted()
    {
        assertQueryFails("CREATE SCHEMA select", ".*mismatched input 'select'\\. Expecting: 'IF', <identifier>.*");
    }

    @Test
    public void testUnquotedDotIsReadAsACatalogQualifier()
    {
        assertQueryFails("CREATE SCHEMA ident.schema", ".*Catalog does not exist: ident.*");
    }

    @Test
    public void testDelimitedColonInASchemaName()
    {
        assertIdentifiersUsable("\"ident:schema_q\"", "ident_colon_table");
    }

    @Test
    public void testDelimitedColonInATableName()
    {
        assertIdentifiersUsable("ident_colon_schema", "\"ident:table_q\"");
    }

    @Test
    public void testDelimitedDollarSignIsUsableInASchemaName()
    {
        assertIdentifiersUsable("\"ident$schema\"", "ident_dollar_table");
    }

    @Test
    public void testDelimitedDollarSignIsRejectedInATableName()
    {
        assertUpdate("CREATE SCHEMA ident_dollar_schema");
        try {
            assertQueryFails("CREATE TABLE ident_dollar_schema.\"ident$table\" (id integer)",
                    ".*Invalid Iceberg table name \\(unknown type 'table'\\): ident\\$table.*");
        }
        finally {
            assertQuerySucceeds("DROP SCHEMA ident_dollar_schema");
        }
    }

    @Test
    public void testDelimitedAtSignIsUsableInASchemaName()
    {
        assertIdentifiersUsable("\"ident@schema\"", "ident_at_table");
    }

    @Test
    public void testDelimitedAtSignIsRejectedInATableName()
    {
        assertUpdate("CREATE SCHEMA ident_at_schema");
        try {
            assertQueryFails("CREATE TABLE ident_at_schema.\"ident@table\" (id integer)",
                    ".*Invalid Iceberg table name: ident@table.*");
        }
        finally {
            assertQuerySucceeds("DROP SCHEMA ident_at_schema");
        }
    }
}
