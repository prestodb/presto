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

#ifndef GRAMMAR_H
#define GRAMMAR_H

typedef struct TOKEN_T {
  int index;
  char word[40];
  int (*handler)(char* s, struct TOKEN_T* t);
} token_t;

#define COMMENT_CHAR '-'
#define STMT_END ';'
int ParseFile(char* szPath);
int FindToken(char* name);
void SetTokens(token_t* t);
char* ProcessStr(char* stmt, token_t* pTokens);
char* SafeStrtok(char* string, char* delims);
extern token_t* pTokens;

#endif /* GRAMMAR_H */
