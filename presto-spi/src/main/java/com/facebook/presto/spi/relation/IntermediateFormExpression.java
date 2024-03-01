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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.SourceLocation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/**
 * Abstract class for a RowExpression which only exists before the plan is finalized.
 * These RowExpressions are compiled to other forms of RowExpression during Optimizer
 * and never reach a worker.
 */
public abstract class IntermediateFormExpression
        extends RowExpression
{
    @JsonCreator
    public IntermediateFormExpression(@JsonProperty("sourceLocation") Optional<SourceLocation> sourceLocation)
    {
        super(sourceLocation);
    }
}
