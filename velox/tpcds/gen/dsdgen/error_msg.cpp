/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#include "velox/tpcds/gen/dsdgen/include/error_msg.h"
#include <stdio.h>
#include <algorithm>
#include <cstdint>
#include <vector>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/grammar_support.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
static int* LN;
static char* FN;

err_msg_t Errors[MAX_ERROR + 2] = {
    {
        EFLG_NO_ARG,
        "",
    },
    {EFLG_STR_ARG, "File '%s' not found"},
    {EFLG_NO_ARG, "Line exceeds maximum leng.h"},
    {EFLG_STR_ARG, "Memory allocation failed %s"},
    {EFLG_STR_ARG, "Syntax Error: \n'%s'"},
    {EFLG_NO_ARG, "Invalid/Out-of-range Argument"},
    {EFLG_STR_ARG, "'%s' is not a unique name"},
    {EFLG_STR_ARG, "'%s' is not a valid name"},
    {EFLG_NO_ARG, "Command parse failed"},
    {EFLG_NO_ARG, "Invalid tag found"},
    {EFLG_STR_ARG, "Read failed on '%s'"},
    {EFLG_NO_ARG, "Too Many Templates!"},
    {EFLG_NO_ARG, "Each workload definition must be in its own file"},
    {EFLG_NO_ARG,
     "Query Class name must be unique within a workload definition"},
    {EFLG_NO_ARG, "Query Template must be unique within a query class"},
    {EFLG_STR_ARG | EFLG_SYSTEM, "Open failed on '%s'"},
    {EFLG_STR_ARG, "%s  not yet implemented"}, /* QERR_NOT_IMPLEMENTED */
    {EFLG_STR_ARG, "string trucated to '%s'"},
    {EFLG_NO_ARG, "Non-terminated string"},
    {EFLG_STR_ARG, "failed to write to '%s'"},
    {EFLG_NO_ARG, "No type vector defined for distribution"},
    {EFLG_NO_ARG, "No weight count defined for distribution"},
    {EFLG_NO_ARG, "No limits defined for pricing calculations"},
    {EFLG_STR_ARG, "Percentage is out of bounds in substitution '%s'"},
    {EFLG_STR_ARG, "Name is not a distribution or table name: '%s'"},
    {EFLG_NO_ARG, "Cannot evaluate expression"},
    {EFLG_STR_ARG,
     "Substitution'%s' is used before being initialized"}, /* QERR_NO_INIT
                                                            */
    {EFLG_NO_ARG,
     "RANGE()/LIST()/ULIST() not supported for NORMAL "
     "distributions"},
    {EFLG_STR_ARG, "Bad Nesting; '%s' not found"},
    {EFLG_STR_ARG, "Include stack overflow when opening '%s'"},
    {EFLG_STR_ARG, "Bad function call: '%s'"},
    {EFLG_STR_ARG, "Bad Hierarchy Call: '%s'"},
    {EFLG_NO_ARG, "Must set types and weights before defining names"},
    {EFLG_NO_ARG, "More than 20 arguments in definition"},
    {EFLG_NO_ARG, "Argument type mismat.h"},
    {EFLG_NO_ARG,
     "RANGE()/LIST()/ULIST() cannot be used in the "
     "same expression"}, /* QERR_RANGE_LIST
                          */
    {EFLG_NO_ARG, "Selected scale factor is NOT valid for result publication"},
    {EFLG_STR_ARG, "Parameter setting failed for '%s'"},
    {EFLG_STR_ARG, "Table %s is being joined without an explicit rule"},
    {EFLG_STR_ARG, "Table %s is not yet fully defined"},
    {EFLG_STR_ARG,
     "Table %s is a child; it is populated during the build of "
     "its parent (e.g., catalog_sales builds catalog returns)"},
    {EFLG_NO_ARG,
     "Command line arguments for dbgen_version exceed 200 "
     "characters; truncated"},
    {EFLG_NO_ARG,
     "A query template list must be supplied using the "
     "INPUT option"}, /* QERR_NO_QUERYLIST
                       */
    {EFLG_NO_ARG,
     "Invalid query number found in permutation!"}, /* QERR_QUERY_RANGE
                                                     */
    {EFLG_NO_ARG,
     "RANGE/LIST/ULIST expressions not valid as "
     "function parameters"}, /* QERR_MODIFIED_PARAM
                              */
    {EFLG_NO_ARG,
     "RANGE/LIST/ULIST truncated to available "
     "values"}, /* QERR_MODIFIED_PARAM
                 */
    {EFLG_NO_ARG,
     "This scale factor is valid for QUALIFICATION "
     "ONLY"}, /* QERR_QUALIFICATION_SCALE
               */
    {EFLG_STR_ARG,
     "Generating %s requires the '-update' option"}, /* QERR_TABLE_UPDATE
                                                      */
    {0, NULL}};

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void ProcessErrorCode(
    int nErrorCode,
    char* szRoutineName,
    char* szParam,
    int nParam) {
  switch (nErrorCode) {
    case QERR_NO_FILE:
      ReportError(QERR_NO_FILE, szParam, 1);
      break;
    case QERR_SYNTAX:
    case QERR_RANGE_ERROR:
    case QERR_NON_UNIQUE:
    case QERR_BAD_NAME:
    case QERR_DEFINE_OVERFLOW:
    case QERR_INVALID_TAG:
    case QERR_READ_FAILED:
    case QERR_NO_MEMORY:
    case QERR_LINE_TOO_LONG:
      ReportError(nErrorCode, szRoutineName, 1);
      break;
  }
  return;
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int ReportError(int nError, const char* msg, int bExit) {
  auto result = fprintf(stderr, "ERROR?!\n");
  if (result < 0)
    perror("sprintf failed");
  return (nError);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int ReportErrorNoLine(int nError, const char* msg, int bExit) {
  if (nError < MAX_ERROR) {
    switch (static_cast<uint32_t>(Errors[-nError].flags) & EFLG_ARG_MASK) {
      case EFLG_NO_ARG: {
        auto result = fprintf(
            stderr,
            "%s: %s\n",
            (bExit) ? "ERROR" : "Warning",
            Errors[-nError].prompt);
        if (result < 0)
          perror("sprintf failed");
        break;
      }
      case EFLG_STR_ARG: {
        auto prompt = Errors[std::min(
                                 static_cast<uint32_t>(-nError),
                                 static_cast<uint32_t>(MAX_ERROR + 1))]
                          .prompt;
        std::vector<char> e_msg(1024 + strlen(prompt) + strlen(msg));
        auto result = snprintf(e_msg.data(), e_msg.size(), prompt, msg);
        if (result < 0)
          perror("sprintf failed");
        result = fprintf(
            stderr, "%s: %s\n", (bExit) ? "ERROR" : "Warning", e_msg.data());
        if (result < 0)
          perror("sprintf failed");
        break;
      }
    }

    if (static_cast<uint32_t>(Errors[static_cast<uint32_t>(-nError)].flags) &
        EFLG_SYSTEM)
      perror(msg);
  }

  if (bExit)
    exit(nError);
  else
    return (nError);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void SetErrorGlobals(char* szFileName, int* nLineNumber) {
  FN = szFileName;
  LN = nLineNumber;

  return;
}
