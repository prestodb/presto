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

#ifndef MISC_H
#define MISC_H
int prep_direct(int dialect);
int close_direct(int dialect);
int pick_tbl(char* dname);
int itostr(char* dest, int i);
int char_op(char* dest, int op, char* s1, char* s2);
void gen_text(
    char* dest,
    int min,
    int max,
    int stream,
    DSDGenContext& dsdGenContext);
int int_op(int* dest, int op, int arg1, int arg2);

char* env_config(char* var, char* dflt);
int a_rnd(int min, int max, int column, char* dest);
#endif
