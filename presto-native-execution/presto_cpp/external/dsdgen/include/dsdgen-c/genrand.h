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

#ifndef GENRAND_H
#define GENRAND_H

#include "address.h"
#include "date.h"
#include "decimal.h"
#include "dist.h"

extern rng_t Streams[];

#define FL_SEED_OVERRUN 0x0001

#define ALPHANUM "abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ0123456789"
#define DIGITS "0123456789"

#define RNG_SEED 19620718

int genrand_integer(
    int* dest,
    int dist,
    int min,
    int max,
    int mean,
    int stream,
    DSDGenContext& dsdGenContext);
int genrand_decimal(
    decimal_t* dest,
    int dist,
    decimal_t* min,
    decimal_t* max,
    decimal_t* mean,
    int stream,
    DSDGenContext& dsdGenContext);
int genrand_date(
    date_t* dest,
    int dist,
    date_t* min,
    date_t* max,
    date_t* mean,
    int stream,
    DSDGenContext& dsdGenContext);
ds_key_t genrand_key(
    ds_key_t* dest,
    int dist,
    ds_key_t min,
    ds_key_t max,
    ds_key_t mean,
    int stream,
    DSDGenContext& dsdGenContext);
int gen_charset(
    char* dest,
    char* set,
    int min,
    int max,
    int stream,
    DSDGenContext& dsdGenContext);
int dump_seeds_ds(int tbl);
void init_rand(DSDGenContext& dsdGenContext);
void skip_random(int s, ds_key_t count, DSDGenContext& dsdGenContext);
int RNGReset(int nTable);
long next_random(int nStream, DSDGenContext& dsdGenContext);
void genrand_email(
    char* pEmail,
    char* pFirst,
    char* pLast,
    int nColumn,
    DSDGenContext& dsdGenContext);
void genrand_ipaddr(char* pDest, int nColumn, DSDGenContext& dsdGenContext);
int genrand_url(char* pDest, int nColumn);
int setSeed(int nStream, int nValue, DSDGenContext& dsdGenContext);
void resetSeeds(int nTable, DSDGenContext& dsdGenContext);

#endif
