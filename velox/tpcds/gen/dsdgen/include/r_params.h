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

#include "velox/tpcds/gen/dsdgen/include/dist.h" // @manual

#ifndef R_PARAMS_H
#define R_PARAMS_H
#define OPT_NONE 0x00
#define OPT_FLG 0x01 /* option is a flag; no parameter */
#define OPT_INT 0x02 /* argument is an integer */
#define OPT_STR 0x04 /* argument is a string */
#define OPT_NOP 0x08 /* flags non-operational options */
#define OPT_SUB 0x10 /* sub-option defined */
#define OPT_ADV 0x20 /* advanced option */
#define OPT_SET \
  0x40 /* not changeable -- used for default/file/command precedence */
#define OPT_DFLT 0x80 /* param set to non-zero default */
#define OPT_MULTI 0x100 /* param may be set repeatedly */
#define OPT_HIDE 0x200 /* hidden option -- not listed in usage */
#define TYPE_MASK 0x07
#endif
/*
 * function declarations
 */
int process_options(int count, const char** args);
const char* get_str(const char* var, DSDGenContext& dsdGenContext);
void set_str(
    const char* param,
    const char* value,
    DSDGenContext& dsdGenContext);
int get_int(const char* var, DSDGenContext& dsdGenContext);
void set_int(const char* var, const char* val, DSDGenContext& dsdGenContext);
double get_dbl(const char* var, DSDGenContext& dsdGenContext);
int is_set(const char* flag, DSDGenContext& dsdGenContext);
void clr_flg(const char* flag, DSDGenContext& dsdGenContext);
int find_table(const char* szParamName, const char* tname);
char* GetParamName(int nParam, DSDGenContext& dsdGenContext);
const char* GetParamValue(int nParam, DSDGenContext& dsdGenContext);
int load_param(int nParam, const char* value, DSDGenContext& dsdGenContext);
int fnd_param(const char* name, DSDGenContext& dsdGenContext);
int init_params(DSDGenContext& dsdGenContext);
int set_option(const char* pname, const char* value);
void load_params(void);
int IsIntParam(const char* szName, DSDGenContext& dsdGenContext);
int IsStrParam(const char* szName, DSDGenContext& dsdGenContext);
