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
package com.facebook.presto.plugin.clp.split.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.google.inject.Inject;

/**
 * Split filter provider for metadata databases implemented with Pinot.
 * <p>
 * Currently uses the same implementation as MySQL. This class exists to allow
 * for future Pinot-specific customizations if needed.
 */
public class ClpPinotSplitFilterProvider
        extends ClpMySqlSplitFilterProvider
{
    @Inject
    public ClpPinotSplitFilterProvider(ClpConfig config)
    {
        super(config);
    }
}
