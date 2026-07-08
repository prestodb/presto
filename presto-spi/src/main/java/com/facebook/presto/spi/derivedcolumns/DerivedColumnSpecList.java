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
package com.facebook.presto.spi.derivedcolumns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * This class stores the derived column information, it is JSON serializable.
 */
public class DerivedColumnSpecList
{
    private final List<DerivedColumnSpec> derivedColumnSpecs;

    @JsonCreator
    public DerivedColumnSpecList(@JsonProperty("derivedColumnSpecs") List<DerivedColumnSpec> derivedColumnSpecs)
    {
        this.derivedColumnSpecs = requireNonNull(derivedColumnSpecs, "derivedColumnSpecs is null");
    }

    public boolean validateFieldIds()
    {
        return derivedColumnSpecs.stream().noneMatch(derivedColumnSpec -> derivedColumnSpec.getDerivedColumnFieldId() < 1);
    }

    @JsonProperty
    public List<DerivedColumnSpec> getDerivedColumnSpecs()
    {
        return derivedColumnSpecs;
    }
}
