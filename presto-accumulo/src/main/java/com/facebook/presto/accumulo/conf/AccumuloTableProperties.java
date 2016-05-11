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
package com.facebook.presto.accumulo.conf;

import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.accumulo.serializers.StringRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

/**
 * Class contains all table properties for the Accumulo connector. Used when creating a table:
 * <br>
 * <br>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (column_mapping = 'b:md:b', external = true);
 */
public final class AccumuloTableProperties
{
    public static final String COLUMN_MAPPING = "column_mapping";
    public static final String INDEX_COLUMNS = "index_columns";
    public static final String EXTERNAL = "external";
    public static final String LOCALITY_GROUPS = "locality_groups";
    public static final String ROW_ID = "row_id";
    public static final String SERIALIZER = "serializer";
    public static final String SCAN_AUTHS = "scan_auths";
    private static final Splitter COLON_SPLITTER = Splitter.on(':').omitEmptyStrings().trimResults();
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private static final Splitter PIPE_SPLITTER = Splitter.on('|').omitEmptyStrings().trimResults();

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public AccumuloTableProperties(AccumuloConfig config)
    {
        PropertyMetadata<String> s1 = stringSessionProperty(COLUMN_MAPPING,
                "Comma-delimited list of column metadata: col_name:col_family:col_qualifier,[...]. "
                        + "Required for external tables. "
                        + "Not setting this property results in auto-generated column names.",
                null, false);

        PropertyMetadata<String> s2 =
                stringSessionProperty(INDEX_COLUMNS,
                        "A comma-delimited list of Presto columns that are indexed in this table's "
                                + "corresponding index table. Default is no indexed columns.",
                        "", false);

        PropertyMetadata<Boolean> s3 = booleanSessionProperty(EXTERNAL,
                "If true, Presto will only do metadata operations for the table. Else, Presto will "
                        + "create and drop Accumulo tables where appropriate. Default false.",
                false, false);

        PropertyMetadata<String> s4 = stringSessionProperty(LOCALITY_GROUPS,
                "List of locality groups to set on the Accumulo table. Only valid on internal tables. "
                        + "String format is locality group name, colon, comma delimited list of Presto "
                        + "column names in the group. Groups are delimited by pipes. Example: "
                        + "group1:colA,colB,colC|group2:colD,colE,colF|etc.... Default is no locality groups.",
                null, false);

        PropertyMetadata<String> s5 = stringSessionProperty(ROW_ID,
                "Presto column name that maps to the Accumulo row ID. Default is the first column.",
                null, false);

        PropertyMetadata<String> s6 =
                new PropertyMetadata<>(SERIALIZER,
                        "Serializer for Accumulo data encodings. Can either be 'default', "
                                + "'string', 'lexicoder', or a Java class name. Default is 'default', i.e. "
                                + "the value from AccumuloRowSerializer.getDefault(), i.e. 'lexicoder'.",
                        VarcharType.VARCHAR, String.class,
                        AccumuloRowSerializer.getDefault().getClass().getName(), false,
                        x -> x.equals("default")
                                ? AccumuloRowSerializer.getDefault().getClass().getName()
                                : (x.equals("string") ? StringRowSerializer.class.getName()
                                : (x.equals("lexicoder")
                                ? LexicoderRowSerializer.class.getName()
                                : (String) x)),
                        object -> object);

        PropertyMetadata<String> s7 = stringSessionProperty(SCAN_AUTHS,
                "Scan-time authorizations set on the batch scanner. Default is all scan authorizations for the user",
                null, false);

        tableProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7);
    }

    /**
     * Gets all available table properties
     *
     * @return A List of table properties
     */
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    /**
     * Gets the value of the column_mapping property and parses it into a map of Presto column name
     * to a pair of strings, the Accumulo column family and qualifier.
     *
     * @param tableProperties The map of table properties
     * @return The column mapping, presto name to (accumulo column family, qualifier)
     */
    public static Map<String, Pair<String, String>> getColumnMapping(
            Map<String, Object> tableProperties)
    {
        String strMapping = (String) tableProperties.get(COLUMN_MAPPING);
        if (strMapping == null) {
            return null;
        }

        // Parse out the column mapping
        // This is a comma-delimited list of "presto column:accumulo fam:accumulo qualifier"
        // triplets
        Map<String, Pair<String, String>> mapping = new HashMap<>();
        for (String m : COMMA_SPLITTER.split(strMapping)) {
            String[] tokens = Iterables.toArray(COLON_SPLITTER.split(m), String.class);

            // If there are three tokens, parse out the mapping
            // Else throw an exception!
            if (tokens.length == 3) {
                mapping.put(tokens[0], Pair.of(tokens[1], tokens[2]));
            }
            else {
                throw new InvalidParameterException(String
                        .format("Mapping of %s contains %d tokens instead of 3", m, tokens.length));
            }
        }

        return mapping;
    }

    /**
     * Gets the list of all indexed columns set in the table properties
     *
     * @param tableProperties The map of table properties
     * @return The list of indexed columns, or an empty list if there are none
     */
    public static List<String> getIndexColumns(Map<String, Object> tableProperties)
    {
        return Arrays.asList(StringUtils.split((String) tableProperties.get(INDEX_COLUMNS), ','));
    }

    /**
     * Gets the configured locality groups for the table.
     *
     * @param tableProperties The map of table properties
     * @return Optional map of locality groups
     */
    public static Optional<Map<String, Set<String>>> getLocalityGroups(Map<String, Object> tableProperties)
    {
        String groupStr = (String) tableProperties.get(LOCALITY_GROUPS);
        if (groupStr == null) {
            return Optional.empty();
        }

        Map<String, Set<String>> groups = new HashMap<>();

        // Split all configured locality groups
        for (String group : PIPE_SPLITTER.split(groupStr)) {
            String[] locGroups = Iterables.toArray(COLON_SPLITTER.split(group), String.class);

            if (locGroups.length != 2) {
                throw new PrestoException(VALIDATION,
                        "Locality groups string is malformed. See documentation for proper format.");
            }

            String grpName = locGroups[0];
            Set<String> colSet = new HashSet<>();
            groups.put(grpName, colSet);

            for (String f : COMMA_SPLITTER.split(locGroups[1])) {
                colSet.add(f);
            }
        }

        return Optional.of(groups);
    }

    /**
     * Gets the configured row ID for the table
     *
     * @param tableProperties The map of table properties
     * @return The row ID, or null if none was specifically set (use the first column)
     */
    public static String getRowId(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(ROW_ID);
    }

    /**
     * Gets the scan authorizations, or null if all of the user's scan authorizations should be used
     *
     * @param tableProperties The map of table properties
     * @return The scan authorizations
     */
    public static Optional<String> getScanAuthorizations(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(SCAN_AUTHS));
    }

    /**
     * Gets the {@link AccumuloRowSerializer} class name to use for this table
     *
     * @param tableProperties The map of table properties
     * @return The name of the AccumuloRowSerializer class
     */
    public static String getSerializerClass(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(SERIALIZER);
    }


    /**
     * Gets a Boolean value indicating whether or not this table is external and Presto should only
     * manage metadata
     *
     * @param tableProperties The map of table properties
     * @return True if the table is external, false otherwise
     */
    public static boolean isExternal(Map<String, Object> tableProperties)
    {
        return (Boolean) tableProperties.get(EXTERNAL);
    }
}
