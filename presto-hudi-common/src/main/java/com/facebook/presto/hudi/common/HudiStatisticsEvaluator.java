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
package com.facebook.presto.hudi.common;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class HudiStatisticsEvaluator
{
    private HudiStatisticsEvaluator() {}

    /**
     * Returns false only if statistics rule out every matching row.
     *
     * File domains must describe the exact physical file. Snapshot selection is
     * the reader's responsibility. Missing domains and incompatible types cannot
     * rule out a match. A true result does not enforce the predicate; callers must
     * preserve row-level filtering.
     */
    public static boolean mayMatch(TupleDomain<String> predicate, Map<String, Domain> fileDomains)
    {
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileDomains, "fileDomains is null");
        if (predicate.isNone()) {
            return false;
        }
        if (predicate.isAll()) {
            return true;
        }

        for (Map.Entry<String, Domain> entry : predicate.getDomains().get().entrySet()) {
            Domain fileDomain = fileDomains.get(entry.getKey());
            Domain queryDomain = entry.getValue();
            if (fileDomain != null && fileDomain.getType().equals(queryDomain.getType()) && !queryDomain.overlaps(fileDomain)) {
                return false;
            }
        }
        return true;
    }
}
