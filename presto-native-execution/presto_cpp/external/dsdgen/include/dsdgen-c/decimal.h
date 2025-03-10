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

#ifndef R_DECIMAL_H
#define R_DECIMAL_H
#include <stdio.h>
#include "config.h"
#include "dist.h"
#include "mathops.h"
#include "porting.h"

#define FL_INIT 0x0004

decimal_t* mk_decimal(int s, int p);

int itodec(decimal_t* dest, int i);
int ftodec(decimal_t* d, double f);
int strtodec(decimal_t* d, char* src);

int dectostr(char* dest, decimal_t* d);
int dectof(double* dest, decimal_t*);
#define dectoi(d) atoi(d->number)

int decimal_t_op(decimal_t* dest, int o, decimal_t* d1, decimal_t* d2);
void print_decimal(int nColumn, decimal_t* d, int s);
void set_precision(decimal_t* d, int sie, int precision);
#define NegateDecimal(d) (d)->number *= -1
#endif /* R_DECIMAL_H */
