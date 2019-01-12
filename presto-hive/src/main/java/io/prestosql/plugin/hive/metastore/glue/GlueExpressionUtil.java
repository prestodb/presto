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
package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

public final class GlueExpressionUtil
{
    private static final Joiner JOINER = Joiner.on(" AND ");
    private static final Set<String> QUOTED_TYPES = ImmutableSet.of("string", "char", "varchar", "date", "timestamp", "binary", "varbinary");

    private GlueExpressionUtil() {}

    /**
     * Build an expression string used for partition filtering in {@link GetPartitionsRequest}
     * <pre>
     * Ex: partition keys: ['a', 'b']
     *     partition values: ['1', '2']
     *     expression: (a='1') AND (b='2')
     *
     * Partial specification ex:
     *      partition values: ['', '2']
     *      expression: (b='2')
     * </pre>
     *
     * @param partitionKeys List of partition keys to filter on
     * @param partitionValues Full or partial list of partition values to filter on. Keys without filter should be empty string.
     */
    public static String buildGlueExpression(List<Column> partitionKeys, List<String> partitionValues)
    {
        if (partitionValues == null || partitionValues.isEmpty()) {
            return null;
        }

        if (partitionKeys == null || partitionValues.size() != partitionKeys.size()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Incorrect number of partition values: " + partitionValues);
        }

        List<String> predicates = new LinkedList<>();
        for (int i = 0; i < partitionValues.size(); i++) {
            if (!Strings.isNullOrEmpty(partitionValues.get(i))) {
                predicates.add(buildPredicate(partitionKeys.get(i), partitionValues.get(i)));
            }
        }

        return JOINER.join(predicates);
    }

    private static String buildPredicate(Column partitionKey, String value)
    {
        if (isQuotedType(partitionKey.getType())) {
            return String.format("(%s='%s')", partitionKey.getName(), value);
        }
        return String.format("(%s=%s)", partitionKey.getName(), value);
    }

    private static boolean isQuotedType(HiveType type)
    {
        return QUOTED_TYPES.contains(type.getTypeSignature().getBase());
    }
}
