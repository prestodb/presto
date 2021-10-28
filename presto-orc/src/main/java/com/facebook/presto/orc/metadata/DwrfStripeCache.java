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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.orc.StripeReader.StripeId;
import io.airlift.slice.Slice;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * DWRF stripe cache with index and footer slices bound to the stripe offset.
 * <p>
 * Depending on the mode the cache can have only index slices, only footer slices,
 * or both. Cache might have data only up to a certain stripe depending on the
 * max stripe cache size set in the writer that creates a given file.
 */
public class DwrfStripeCache
{
    private final DwrfStripeCacheMode mode;

    /**
     * Stripe index streams slices by the stripe offset.
     */
    private final Map<Long, Slice> indexSlices;

    /**
     * Stripe footer slices by the stripe offset.
     */
    private final Map<Long, Slice> footerSlices;

    public DwrfStripeCache(DwrfStripeCacheMode mode, Map<Long, Slice> indexSlices, Map<Long, Slice> footerSlices)
    {
        this.mode = requireNonNull(mode, "mode is null");
        this.indexSlices = requireNonNull(indexSlices, "indexSlices is null");
        this.footerSlices = requireNonNull(footerSlices, "footerSlices is null");
    }

    /**
     * Get a slice containing an index slice for a given stripe.
     */
    public Optional<Slice> getIndexStreamsSlice(StripeId stripeId)
    {
        if (mode.hasIndex()) {
            Slice slice = indexSlices.get(stripeId.getOffset());
            return Optional.ofNullable(slice);
        }
        return Optional.empty();
    }

    /**
     * Get a slice containing a stripe footer for a given stripe.
     */
    public Optional<Slice> getStripeFooterSlice(StripeId stripeId, int footerLength)
    {
        if (mode.hasFooter()) {
            Slice slice = footerSlices.get(stripeId.getOffset());
            if (slice != null) {
                checkState(footerLength == slice.length(),
                        "Requested footer size %s for stripeId %s does not match the cached footer slice size %s",
                        footerLength,
                        stripeId,
                        slice.length());
                return Optional.of(slice);
            }
        }
        return Optional.empty();
    }
}
