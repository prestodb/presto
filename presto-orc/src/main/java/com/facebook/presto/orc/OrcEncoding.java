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
package com.facebook.presto.orc;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.DwrfMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.orc.metadata.OrcMetadataWriter;

public enum OrcEncoding
{
    ORC {
        @Override
        public MetadataReader createMetadataReader(RuntimeStats runtimeStats)
        {
            return new OrcMetadataReader(runtimeStats);
        }

        @Override
        public MetadataWriter createMetadataWriter()
        {
            return new OrcMetadataWriter();
        }
    },
    DWRF {
        @Override
        public MetadataReader createMetadataReader(RuntimeStats runtimeStats)
        {
            return new DwrfMetadataReader(runtimeStats);
        }

        @Override
        public MetadataWriter createMetadataWriter()
        {
            return new DwrfMetadataWriter();
        }
    };

    public abstract MetadataReader createMetadataReader(RuntimeStats runtimeStats);

    public abstract MetadataWriter createMetadataWriter();
}
