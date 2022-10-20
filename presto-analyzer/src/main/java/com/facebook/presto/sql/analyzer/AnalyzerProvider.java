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

package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.PrestoException;

import javax.inject.Inject;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_ANALYZER_TYPE;
import static com.facebook.presto.sql.analyzer.AnalyzerType.BUILTIN;
import static com.facebook.presto.sql.analyzer.AnalyzerType.NATIVE;
import static java.util.Objects.requireNonNull;

//TODO: Better name?
public class AnalyzerProvider
{
    private QueryPreparer builtInQueryPreparer;
    private QueryPreparer nativeInQueryPreparer;

    @Inject
    public AnalyzerProvider(BuiltInQueryPreparer builtInQueryPreparer, NativeQueryPreparer nativeInQueryPreparer)
    {
        this.builtInQueryPreparer = requireNonNull(builtInQueryPreparer, "builtInQueryPreparer is null");
        this.nativeInQueryPreparer = requireNonNull(nativeInQueryPreparer, "nativeInQueryPreparer is null");
    }

    public QueryPreparer getQueryPreparer(AnalyzerType analyzerType) {
        requireNonNull(analyzerType, "AnalyzerType is null");

        if(analyzerType == BUILTIN) return builtInQueryPreparer;
        if(analyzerType == NATIVE) return nativeInQueryPreparer;

        throw new PrestoException(UNSUPPORTED_ANALYZER_TYPE, "Unsupported analyzer type: " + analyzerType);
    }
}
