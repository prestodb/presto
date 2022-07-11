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
package com.facebook.presto.hive.functions.gen;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static java.util.Locale.ENGLISH;

public class AggregationUtils
{
    private AggregationUtils() {}

    public static String generateAggregationName(String baseName, TypeSignature outputType, List<TypeSignature> inputTypes)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, getAbbreviatedTypeName(outputType)));
        for (TypeSignature inputType : inputTypes) {
            sb.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, getAbbreviatedTypeName(inputType)));
        }
        sb.append(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, baseName.toLowerCase(ENGLISH)));

        return sb.toString();
    }
    private static String getAbbreviatedTypeName(TypeSignature type)
    {
        String typeName = type.toString();
        if (typeName.length() > 10) {
            return typeName.substring(0, 10);
        }
        return typeName;
    }

    public static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(List<Type> inputTypes)
    {
        ImmutableList.Builder<AggregationMetadata.ParameterMetadata> builder = new ImmutableList.Builder<>();
        builder.add(new AggregationMetadata.ParameterMetadata(STATE));
        for (Type inputType : inputTypes) {
            builder.add(new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, inputType));
        }
        builder.add(new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
        return builder.build();
    }

    // used by aggregation compiler
    @SuppressWarnings("UnusedDeclaration")
    public static Block extractMaskBlock(int maskChannel, Page page)
    {
        if (maskChannel < 0) {
            return null;
        }
        Block maskBlock = page.getBlock(maskChannel);
        if (page.getPositionCount() > 0 && maskBlock instanceof RunLengthEncodedBlock && CompilerOperations.testMask(maskBlock, 0)) {
            return null; // filter out RLE true blocks to bypass unnecessary mask checks
        }
        return maskBlock;
    }
}
