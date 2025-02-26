/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
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
char* gen_text(
    char* dest,
    int min,
    int max,
    int stream,
    DSDGenContext& dsdGenContext);
int int_op(int* dest, int op, int arg1, int arg2);

char* env_config(char* var, char* dflt);
int a_rnd(int min, int max, int column, char* dest);
#endif
