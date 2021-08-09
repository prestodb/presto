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
package com.facebook.presto.tablestore;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.tablestore.TablestoreSessionProperties.MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT;
import static com.facebook.presto.tablestore.TablestoreSessionProperties.MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class IndexSelectionStrategy
{
    private static final String INDEX_HINT_MESSAGE = "use hints like: " +
            "1)use index if possible -> 'tablestore-index-selection-strategy=auto' " +
            "2)do not use any index, default -> 'tablestore-index-selection-strategy=none' " +
            "3)use indexes of tables that specified -> 'tablestore-index-selection-strategy=[db1.table1, table2, ...]' " +
            "4)use heuristic rule of max matched rows -> 'tablestore-index-selection-strategy=threshold:1000' " +
            "5)use heuristic rule of max matched percentage -> 'tablestore-index-selection-strategy=threshold:5%'";
    public static final IndexSelectionStrategy NONE = new IndexSelectionStrategy(Type.NONE);
    public static final IndexSelectionStrategy AUTO = new IndexSelectionStrategy(Type.AUTO);

    enum Type
    {
        AUTO("auto"),
        NONE("none"),
        CUSTOM("custom"),
        THRESHOLD("threshold");

        private String code;

        Type(String code)
        {
            this.code = code;
        }

        public String getCode()
        {
            return code;
        }
    }

    private final Type type;
    private int maxPercentage = -1;
    private int maxRows = -1;
    private Set<SchemaTableName> tables = Collections.emptySet();

    private IndexSelectionStrategy(Type type)
    {
        this.type = type;
    }

    public Set<SchemaTableName> getTables()
    {
        if (type == Type.CUSTOM || type == Type.NONE) {
            return tables;
        }
        throw new IllegalStateException("Can't enumerate all the tables for '" + type.getCode() + "' type");
    }

    public boolean isMaxRowsMode()
    {
        return type == Type.THRESHOLD && maxRows > 0;
    }

    public int getMaxPercentage()
    {
        if (type == Type.THRESHOLD && maxPercentage > 0) {
            return maxPercentage;
        }
        throw new IllegalStateException("Can't get max percent for '" + type.getCode() + "' type");
    }

    public int getMaxRows()
    {
        if (type == Type.THRESHOLD && maxRows > 0) {
            return maxRows;
        }
        throw new IllegalStateException("Can't get max rows for '" + type.getCode() + "' type");
    }

    static IndexSelectionStrategy custom(@Nonnull Set<SchemaTableName> tables)
    {
        if (tables.size() == 0) {
            return NONE;
        }
        else {
            IndexSelectionStrategy indexSelectionStrategy = new IndexSelectionStrategy(Type.CUSTOM);
            indexSelectionStrategy.tables = ImmutableSet.copyOf(tables);
            return indexSelectionStrategy;
        }
    }

    static IndexSelectionStrategy thresholdWithPercent(int maxPercent)
    {
        IndexSelectionStrategy indexSelectionStrategy = new IndexSelectionStrategy(Type.THRESHOLD);
        indexSelectionStrategy.maxPercentage = maxPercent;
        return indexSelectionStrategy;
    }

    static IndexSelectionStrategy thresholdWithRows(int maxRows)
    {
        IndexSelectionStrategy indexSelectionStrategy = new IndexSelectionStrategy(Type.THRESHOLD);
        indexSelectionStrategy.maxRows = maxRows;
        return indexSelectionStrategy;
    }

    public boolean isContained(@Nonnull SchemaTableName table)
    {
        if (this == AUTO || this.type == Type.THRESHOLD) {
            return true;
        }
        if (this == NONE) {
            return false;
        }
        return tables.contains(table);
    }

    @Override
    public String toString()
    {
        if (type == Type.AUTO || type == Type.NONE) {
            return type.getCode();
        }
        if (type == Type.THRESHOLD) {
            String str = type.getCode();
            str += maxPercentage > 0 ? ":" + maxPercentage + "%" : "";
            str += maxRows > 0 ? ":" + maxRows : "";
            return str;
        }
        Optional<String> str = tables.stream().map(SchemaTableName::toString).reduce((a, b) -> a + "," + b);
        return "[" + str.orElse("") + "]";
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static IndexSelectionStrategy parse(Optional<String> currentConnectionSchema, String hintKey, String hintValue)
    {
        if (isBlank(hintValue)) {
            return NONE;
        }
        String hint = hintValue.trim().toLowerCase(ENGLISH);
        if (Type.AUTO.getCode().equals(hint)) {
            return AUTO;
        }
        if (Type.NONE.getCode().equals(hint)) {
            return NONE;
        }
        String pattern = "^" + Type.THRESHOLD.getCode() + ":\\s*(\\d+)\\s*(%)?$";
        if (hint.matches(pattern)) {
            Matcher matcher = Pattern.compile(pattern).matcher(hint);
            checkState(matcher.find(), "Invalid 'threshold' hint value '" + hintValue + "' of hint key '" + hintKey + "', regular pattern='" + pattern + "'");

            int integer = Integer.parseInt(matcher.group(1));
            String percent = matcher.group(2);

            if (percent != null) {
                if (integer < 1 || integer > MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT) {
                    throw new IllegalArgumentException(format("Invalid 'threshold:${maxPercent}' hint value: '%s', because [%d] is not in the range [1, %d]", hintValue, integer, MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT));
                }
                return thresholdWithPercent(integer);
            }
            else {
                if (integer < 1 || integer > MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS) {
                    throw new IllegalArgumentException(format("Invalid 'threshold:${maxRows}' hint value: '%s', because [%d] is not in the range [1, %d]", hintValue, integer, MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS));
                }
                return thresholdWithRows(integer);
            }
        }
        if (hint.matches("^\\[[^\\[\\]]*]$")) {
            hint = hint.substring(1, hint.length() - 1).trim();
            if (isBlank(hint)) {
                return NONE;
            }
            String[] schemaTableArray = hint.split("[ ;,]+");
            Set<SchemaTableName> tables = Arrays.stream(schemaTableArray)
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .map(tableName -> checkAndAssemblySchemaTableName(currentConnectionSchema, hintKey, tableName))
                    .collect(Collectors.toSet());
            return IndexSelectionStrategy.custom(tables);
        }

        throw new IllegalArgumentException("Invalid hint value '" + hint + "' of hint key '" + hintKey + "', " + INDEX_HINT_MESSAGE);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static SchemaTableName checkAndAssemblySchemaTableName(Optional<String> currentConnectionSchema,
            String hintKey,
            String tableName)
    {
        String schemaAndTableName;
        if (!tableName.contains(".")) { //'schema'.'table'
            schemaAndTableName = currentConnectionSchema.<IllegalArgumentException>orElseThrow(() -> {
                throw new IllegalArgumentException("Can't obtain the schema of the table[" + tableName + "] from current connection for hint '" + hintKey + "'");
            }) + "." + tableName;
        }
        else {
            schemaAndTableName = tableName;
        }

        String[] parts = schemaAndTableName.split("\\.");
        return new SchemaTableName(parts[0], parts[1]);
    }

    public Type getType()
    {
        return type;
    }
}
