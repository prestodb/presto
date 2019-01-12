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
package io.prestosql.spi.function;

import io.prestosql.spi.block.Block;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A function annotated with &#64;CombineFunction is one that will be used
 * to combine two states into one in aggregation functions.
 * <p>
 * The function should take two arguments. And it should merge both arguments
 * into the first one.
 * <p>
 * The second argument is guaranteed to be the output of
 * {@link AccumulatorStateSerializer#deserialize(Block, int, Object)}.
 * As a result, the implementation of {@code deserialize} method likely
 * provides some application-specific guarantees to the second argument.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface CombineFunction
{
}
