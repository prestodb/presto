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

#ifndef R_DATE_H
#define R_DATE_H

#include "mathops.h"

typedef struct DATE_T {
  int flags;
  int year;
  int month;
  int day;
  int julian;
} date_t;

date_t* mk_date(void);

int jtodt(date_t* dest, int i);
int strtodt(date_t* dest, char* s);
date_t* strtodate(char* str);
int strtotime(char* str);

char* dttostr(date_t* d);
int dttoj(date_t* d);

int date_t_op(date_t* dest, int o, date_t* d1, date_t* d2);
int set_dow(date_t* d);
int is_leap(int year);
int day_number(date_t* d);
int date_part(date_t* d, int p);
int set_outfile(int i);
int getDateWeightFromJulian(int jDay, int nDistribution);
#define CENTURY_SHIFT 20 /* years before this are assumed to be 2000's */
/*
 * DATE OPERATORS
 */
#define OP_FIRST_DOM 0x01 /* get date of first day of current month */
#define OP_LAST_DOM \
  0x02 /* get date of last day of current month; LY == 2/28) */
#define OP_SAME_LY 0x03 /* get date for same day/month, last year */
#define OP_SAME_LQ 0x04 /* get date for same offset in the prior quarter */

extern char* weekday_names[];

#endif /* R_DATE_H */
