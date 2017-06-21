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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.type.ListLiteralType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;

public class ListLiteralCast
{
    private ListLiteralCast()
    {
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(ListLiteralType.NAME)
    public static List<Integer> castArrayToListLiteral(@SqlType("array(integer)") Block array)
    {
        ImmutableList.Builder<Integer> listBuilder = ImmutableList.builder();
        for (int i = 0; i < array.getPositionCount(); i++) {
            listBuilder.add(array.getInt(i, 0));
        }

        return listBuilder.build();
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(ListLiteralType.NAME)
    public static List<List<Integer>> castArrayOfArraysToListLiteral(@SqlType("array(array(integer))") Block arrayOfArrays)
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ImmutableList.Builder<List<Integer>> outerListBuilder = ImmutableList.builder();
        for (int i = 0; i < arrayOfArrays.getPositionCount(); i++) {
            Block subArray = arrayType.getObject(arrayOfArrays, i);
            outerListBuilder.add(castArrayToListLiteral(subArray));
        }

        return outerListBuilder.build();
    }
}
