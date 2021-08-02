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

public enum SemanticErrorCode
{
    MUST_BE_AGGREGATE_OR_GROUP_BY,
    NESTED_AGGREGATION,
    NESTED_WINDOW,
    WINDOW_FUNCTION_ORDERBY_LITERAL,
    MUST_BE_WINDOW_FUNCTION,
    MUST_BE_AGGREGATION_FUNCTION,
    WINDOW_REQUIRES_OVER,
    INVALID_WINDOW_FRAME,
    MUST_BE_COLUMN_REFERENCE,

    MISSING_CATALOG,
    MISSING_SCHEMA,
    MISSING_TABLE,
    MISSING_MATERIALIZED_VIEW,
    MISSING_COLUMN,
    MISMATCHED_COLUMN_ALIASES,
    NOT_SUPPORTED,

    CATALOG_NOT_SPECIFIED,
    SCHEMA_NOT_SPECIFIED,

    INVALID_SCHEMA_NAME,

    SCHEMA_ALREADY_EXISTS,
    TABLE_ALREADY_EXISTS,
    COLUMN_ALREADY_EXISTS,
    MATERIALIZED_VIEW_ALREADY_EXISTS,

    DUPLICATE_RELATION,

    TYPE_MISMATCH,
    AMBIGUOUS_ATTRIBUTE,
    MISSING_ATTRIBUTE,
    INVALID_ORDINAL,
    INVALID_LITERAL,

    FUNCTION_NOT_FOUND,
    INVALID_FUNCTION_NAME,
    DUPLICATE_PARAMETER_NAME,

    ORDER_BY_MUST_BE_IN_SELECT,
    ORDER_BY_MUST_BE_IN_AGGREGATE,
    REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING,
    REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
    NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT,

    CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,

    WILDCARD_WITHOUT_FROM,

    MISMATCHED_SET_COLUMN_TYPES,

    MULTIPLE_FIELDS_FROM_SUBQUERY,

    DUPLICATE_COLUMN_NAME,
    COLUMN_NAME_NOT_SPECIFIED,

    COLUMN_TYPE_UNKNOWN,

    EXPRESSION_NOT_CONSTANT,

    VIEW_PARSE_ERROR,
    VIEW_ANALYSIS_ERROR,
    VIEW_IS_STALE,
    VIEW_IS_RECURSIVE,
    MATERIALIZED_VIEW_IS_RECURSIVE,

    NON_NUMERIC_SAMPLE_PERCENTAGE,

    SAMPLE_PERCENTAGE_OUT_OF_RANGE,

    INVALID_PROCEDURE_ARGUMENTS,

    INVALID_SESSION_PROPERTY,
    INVALID_TRANSACTION_MODE,

    INVALID_PRIVILEGE,

    AMBIGUOUS_FUNCTION_CALL,

    INVALID_PARAMETER_USAGE,

    STANDALONE_LAMBDA,

    DUPLICATE_PROPERTY,

    ROLE_ALREADY_EXIST,
    MISSING_ROLE,

    TOO_MANY_GROUPING_SETS,

    INVALID_OFFSET_ROW_COUNT,
}
