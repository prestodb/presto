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
package io.prestosql.operator.aggregation;

import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.prestosql.operator.aggregation.DecimalAverageAggregation.average;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDecimalAverageAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final DecimalType TYPE = createDecimalType(38, 0);

    private LongDecimalWithOverflowAndLongState state;

    @BeforeMethod
    public void setUp()
    {
        state = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();
    }

    @Test
    public void testOverflow()
    {
        addToState(state, TWO.pow(126));

        assertEquals(state.getLong(), 1);
        assertEquals(state.getOverflow(), 0);
        assertEquals(state.getLongDecimal(), unscaledDecimal(TWO.pow(126)));

        addToState(state, TWO.pow(126));

        assertEquals(state.getLong(), 2);
        assertEquals(state.getOverflow(), 1);
        assertEquals(state.getLongDecimal(), unscaledDecimal(0));

        assertEquals(average(state, TYPE), new BigDecimal(TWO.pow(126)));
    }

    @Test
    public void testUnderflow()
    {
        addToState(state, TWO.pow(126).negate());

        assertEquals(state.getLong(), 1);
        assertEquals(state.getOverflow(), 0);
        assertEquals(state.getLongDecimal(), unscaledDecimal(TWO.pow(126).negate()));

        addToState(state, TWO.pow(126).negate());

        assertEquals(state.getLong(), 2);
        assertEquals(state.getOverflow(), -1);
        assertEquals(UnscaledDecimal128Arithmetic.compare(state.getLongDecimal(), unscaledDecimal(0)), 0);

        assertEquals(average(state, TYPE), new BigDecimal(TWO.pow(126).negate()));
    }

    @Test
    public void testUnderflowAfterOverflow()
    {
        addToState(state, TWO.pow(126));
        addToState(state, TWO.pow(126));
        addToState(state, TWO.pow(125));

        assertEquals(state.getOverflow(), 1);
        assertEquals(state.getLongDecimal(), unscaledDecimal(TWO.pow(125)));

        addToState(state, TWO.pow(126).negate());
        addToState(state, TWO.pow(126).negate());
        addToState(state, TWO.pow(126).negate());

        assertEquals(state.getOverflow(), 0);
        assertEquals(state.getLongDecimal(), unscaledDecimal(TWO.pow(125).negate()));

        assertEquals(average(state, TYPE), new BigDecimal(TWO.pow(125).negate().divide(BigInteger.valueOf(6))));
    }

    @Test
    public void testCombineOverflow()
    {
        addToState(state, TWO.pow(125));
        addToState(state, TWO.pow(126));

        LongDecimalWithOverflowAndLongState otherState = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();

        addToState(otherState, TWO.pow(125));
        addToState(otherState, TWO.pow(126));

        DecimalAverageAggregation.combine(state, otherState);
        assertEquals(state.getLong(), 4);
        assertEquals(state.getOverflow(), 1);
        assertEquals(state.getLongDecimal(), unscaledDecimal(TWO.pow(126)));

        BigInteger expectedAverage = BigInteger.ZERO
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(125))
                .add(TWO.pow(125))
                .divide(BigInteger.valueOf(4));

        assertEquals(average(state, TYPE), new BigDecimal(expectedAverage));
    }

    @Test
    public void testCombineUnderflow()
    {
        addToState(state, TWO.pow(125).negate());
        addToState(state, TWO.pow(126).negate());

        LongDecimalWithOverflowAndLongState otherState = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();

        addToState(otherState, TWO.pow(125).negate());
        addToState(otherState, TWO.pow(126).negate());

        DecimalAverageAggregation.combine(state, otherState);
        assertEquals(state.getLong(), 4);
        assertEquals(state.getOverflow(), -1);
        assertEquals(state.getLongDecimal(), unscaledDecimal(TWO.pow(126).negate()));

        BigInteger expectedAverage = BigInteger.ZERO
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(125))
                .add(TWO.pow(125))
                .negate()
                .divide(BigInteger.valueOf(4));

        assertEquals(average(state, TYPE), new BigDecimal(expectedAverage));
    }

    private static void addToState(LongDecimalWithOverflowAndLongState state, BigInteger value)
    {
        BlockBuilder blockBuilder = TYPE.createFixedSizeBlockBuilder(1);
        TYPE.writeSlice(blockBuilder, unscaledDecimal(value));

        DecimalAverageAggregation.inputLongDecimal(TYPE, state, blockBuilder.build(), 0);
    }
}
