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
import com.facebook.presto.common.type.Type;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface HudiColumnStatsReader
{
    // Keys contain the table-relative partition path and the physical filename.
    Map<Pair<String, String>, Map<String, Domain>> readColumnStats(
            List<Pair<String, String>> files,
            Map<String, Type> columns)
            throws IOException;
}
