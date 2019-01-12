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

package io.prestosql.type.setdigest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.io.UncheckedIOException;
import java.util.Map;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.type.setdigest.SetDigest.exactIntersectionCardinality;

public final class SetDigestFunctions
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private SetDigestFunctions()
    {
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(SetDigestType.NAME) Slice digest)
    {
        return SetDigest.newInstance(digest).cardinality();
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long intersectionCardinality(@SqlType(SetDigestType.NAME) Slice slice1, @SqlType(SetDigestType.NAME) Slice slice2)
    {
        SetDigest digest1 = SetDigest.newInstance(slice1);
        SetDigest digest2 = SetDigest.newInstance(slice2);

        if (digest1.isExact() && digest2.isExact()) {
            return exactIntersectionCardinality(digest1, digest2);
        }

        long cardinality1 = digest1.cardinality();
        long cardinality2 = digest2.cardinality();
        double jaccard = SetDigest.jaccardIndex(digest1, digest2);
        digest1.mergeWith(digest2);
        long result = Math.round(jaccard * digest1.cardinality());

        // When one of the sets is much smaller than the other and approaches being a true
        // subset of the other, the computed cardinality may exceed the cardinality estimate
        // of the smaller set. When this happens the cardinality of the smaller set is obviously
        // a better estimate of the one computed with the Jaccard Index.
        return Math.min(result, Math.min(cardinality1, cardinality2));
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double jaccardIndex(@SqlType(SetDigestType.NAME) Slice slice1, @SqlType(SetDigestType.NAME) Slice slice2)
    {
        SetDigest digest1 = SetDigest.newInstance(slice1);
        SetDigest digest2 = SetDigest.newInstance(slice2);

        return SetDigest.jaccardIndex(digest1, digest2);
    }

    @ScalarFunction
    @SqlType("map(bigint,smallint)")
    public static Block hashCounts(@TypeParameter("map<bigint,smallint>") Type mapType, @SqlType(SetDigestType.NAME) Slice slice)
    {
        SetDigest digest = SetDigest.newInstance(slice);

        // Maybe use static BlockBuilderStatus in order avoid `new`?
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<Long, Short> entry : digest.getHashCounts().entrySet()) {
            BIGINT.writeLong(singleMapBlockBuilder, entry.getKey());
            SMALLINT.writeLong(singleMapBlockBuilder, entry.getValue());
        }
        blockBuilder.closeEntry();

        return (Block) mapType.getObject(blockBuilder, 0);
    }

    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashCounts(@SqlType(SetDigestType.NAME) Slice slice)
    {
        SetDigest digest = SetDigest.newInstance(slice);

        try {
            return Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(digest.getHashCounts()));
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
