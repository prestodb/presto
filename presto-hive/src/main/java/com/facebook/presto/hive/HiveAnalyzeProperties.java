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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class HiveAnalyzeProperties
{
    public static final String PARTITIONS_PROPERTY = "partitions";

    private final List<PropertyMetadata<?>> analyzeProperties;

    @Inject
    public HiveAnalyzeProperties(TypeManager typeManager)
    {
        analyzeProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITIONS_PROPERTY,
                        "Partitions to be analyzed",
                        typeManager.getType(parseTypeSignature("array(array(varchar))")),
                        List.class,
                        null,
                        false,
                        HiveAnalyzeProperties::decodePartitionLists,
                        value -> value));
    }

    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @SuppressWarnings("unchecked")
    public static Optional<List<List<String>>> getPartitionList(Map<String, Object> properties)
    {
        List<List<String>> partitions = (List<List<String>>) properties.get(PARTITIONS_PROPERTY);
        return partitions == null ? Optional.empty() : Optional.of(partitions);
    }

    private static List<List<String>> decodePartitionLists(Object object)
    {
        if (object == null) {
            return null;
        }

        // replace null partition value with hive default partition
        return ImmutableList.copyOf(((Collection<?>) object).stream()
                .peek(HiveAnalyzeProperties::throwIfNull)
                .map(partition -> ((Collection<?>) partition).stream()
                        .map(name -> firstNonNull((String) name, HIVE_DEFAULT_DYNAMIC_PARTITION))
                        .collect(toImmutableList()))
                .collect(toImmutableSet()));
    }

    private static void throwIfNull(Object object)
    {
        if (object == null) {
            throw new PrestoException(INVALID_ANALYZE_PROPERTY, "Invalid null value in analyze partitions property");
        }
    }
}
