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
package io.prestosql.operator;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public interface TrackingLookupSourceSupplier
{
    LookupSource getLookupSource();

    OuterPositionIterator getOuterPositionIterator();

    static TrackingLookupSourceSupplier nonTracking(Supplier<LookupSource> lookupSourceSupplier)
    {
        requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        return new TrackingLookupSourceSupplier()
        {
            @Override
            public LookupSource getLookupSource()
            {
                return lookupSourceSupplier.get();
            }

            @Override
            public OuterPositionIterator getOuterPositionIterator()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
