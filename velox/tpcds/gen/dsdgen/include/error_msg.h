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

#define QERR_OK 0
#define QERR_NO_FILE -1
#define QERR_LINE_TOO_LONG -2
#define QERR_NO_MEMORY -3
#define QERR_SYNTAX -4
#define QERR_RANGE_ERROR -5
#define QERR_NON_UNIQUE -6
#define QERR_BAD_NAME -7
#define QERR_DEFINE_OVERFLOW -8
#define QERR_INVALID_TAG -9
#define QERR_READ_FAILED -10
#define QERR_TEMPLATE_OVERFLOW -11
#define QERR_ONE_WORKLOAD -12
#define QERR_CLASS_REDEFINE -13
#define QERR_DUP_QUERY -14
#define QERR_OPEN_FAILED -15
#define QERR_NOT_IMPLEMENTED -16
#define QERR_STR_TRUNCATED -17
#define QERR_BAD_STRING -18
#define QERR_WRITE_FAILED -19
#define QERR_NO_TYPE -20
#define QERR_NO_WEIGHT -21
#define QERR_NO_LIMIT -22
#define QERR_BAD_PERCENT -23
#define QERR_ROWCOUNT_NAME -24
#define QERR_NO_EXPR -25
#define QERR_NO_INIT -26
#define QERR_NO_NORMAL_RANGE -27
#define QERR_UNBALANCED -28
#define QERR_INCLUDE_OVERFLOW -29
#define QERR_BAD_PARAMS -30
#define DBGEN_ERROR_HIERACHY_ORDER -31
#define QERR_NAMES_EARLY -32
#define QERR_ARG_OVERFLOW -33
#define QERR_INVALID_ARG -34
#define QERR_RANGE_LIST -35
#define QERR_BAD_SCALE -36
#define QERR_BAD_PARAM -37
#define QERR_BAD_JOIN -38
#define QERR_TABLE_NOP -39
#define QERR_TABLE_CHILD -40
#define QERR_CMDLINE_TOO_LONG -41
#define QERR_NO_QUERYLIST -42
#define QERR_QUERY_RANGE -43
#define QERR_MODIFIED_PARAM -44
#define QERR_RANGE_OVERRUN -45
#define QERR_QUALIFICATION_SCALE -46
#define QERR_TABLE_UPDATE -47
#define MAX_ERROR 47

typedef struct ERR_MSG_T {
  int flags;
  const char* prompt;
} err_msg_t;

/*  Flag determine formating */
#define EFLG_NO_ARG 0x0000
#define EFLG_STR_ARG 0x0001
#define EFLG_ARG_MASK 0x0001

#define EFLG_SYSTEM 0x0002

int ReportError(int nError, const char* arg, int bExit);
int ReportErrorNoLine(int nError, const char* arg, int bExit);
void SetErrorGlobals(char* szFileName, int* pnLineNumber);
