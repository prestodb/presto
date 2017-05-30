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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.raptorx.util.Reflection.getMethod;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class CreateDistributionProcedure
        implements Provider<Procedure>
{
    public static final int MAX_BUCKETS = 100_000;

    private final TypeManager typeManager;
    private final Metadata metadata;
    private final NodeSupplier nodeSupplier;

    @Inject
    public CreateDistributionProcedure(TypeManager typeManager, Metadata metadata, NodeSupplier nodeSupplier)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "create_distribution",
                ImmutableList.<Argument>builder()
                        .add(new Argument("name", "varchar"))
                        .add(new Argument("bucket_count", "bigint"))
                        .add(new Argument("column_types", "array(varchar)"))
                        .build(),
                getMethod(getClass(), "createDistribution").bindTo(this));
    }

    @SuppressWarnings("unused")
    public void createDistribution(String name, long count, List<String> typeNames)
    {
        validate(!name.isEmpty(), "Distribution name is empty");
        validate(count > 0, "Bucket count must be greater than zero");
        validate(count <= MAX_BUCKETS, "Bucket count must be no more than " + MAX_BUCKETS);
        validate(!typeNames.isEmpty(), "At least one column type must be specified");

        List<Type> types = typeNames.stream()
                .map(typeName -> parseType(typeName)
                        .map(typeManager::getType)
                        .orElseThrow(() -> new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Invalid column type: " + typeName)))
                .peek(HashingBucketFunction::validateBucketType)
                .collect(toImmutableList());

        Set<Long> nodes = nodeSupplier.getRequiredWorkerNodes().stream()
                .map(RaptorNode::getNodeId)
                .collect(toSet());

        metadata.createDistribution(Optional.of(name), types, bucketNodes(nodes, toIntExact(count)));
    }

    public static List<Long> bucketNodes(Set<Long> nodeIds, int bucketCount)
    {
        return cyclingShuffledStream(nodeIds)
                .limit(bucketCount)
                .collect(toImmutableList());
    }

    private static Optional<TypeSignature> parseType(String typeName)
    {
        try {
            return Optional.of(parseTypeSignature(typeName));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static void validate(boolean condition, String message)
    {
        if (!condition) {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, message);
        }
    }

    private static <T> Stream<T> cyclingShuffledStream(Collection<T> collection)
    {
        List<T> list = new ArrayList<>(collection);
        Collections.shuffle(list);
        return Stream.generate(() -> list).flatMap(List::stream);
    }
}
