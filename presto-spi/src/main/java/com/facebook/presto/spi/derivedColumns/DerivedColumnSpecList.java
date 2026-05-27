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
package com.facebook.presto.spi.derivedColumns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DerivedColumnSpecList
{
    private final List<DerivedColumnSpec> derivedColumnSpecs;

    @JsonCreator
    public DerivedColumnSpecList(@JsonProperty("derivedColumnSpecs") List<DerivedColumnSpec> derivedColumnSpecs)
    {
        this.derivedColumnSpecs = derivedColumnSpecs;
    }

    @JsonProperty
    public List<DerivedColumnSpec> getDerivedColumnSpecs()
    {
        return derivedColumnSpecs;
    }

    @Override
    public String toString()
    {
        return "DerivedColumnSpecList{" +
                "derivedColumnSpecs=" + derivedColumnSpecs.stream().map(DerivedColumnSpec::toString).reduce((x, y) -> x + "," + y).orElse("{}") +
                '}';
    }
}
