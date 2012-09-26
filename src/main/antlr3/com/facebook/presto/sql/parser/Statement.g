/*
 *  Copyright 2008, 2010 David Phillips
 *  Copyright 2012 Facebook, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

grammar Statement;

options {
    output = AST;
    ASTLabelType = CommonTree;
}

tokens {
    STATEMENT_LIST;
    GROUPBY;
    ORDERBY;
    SORT_ITEM;
    QUERY;
    ALL_COLUMNS;
    SELECT_LIST;
    SELECT_ITEM;
    CORR_SPEC;
    CORR_LIST;
    SUBQUERY;
    TABLE;
    JOINED_TABLE;
    QUALIFIED_JOIN;
    CROSS_JOIN;
    INNER_JOIN;
    LEFT_JOIN;
    RIGHT_JOIN;
    FULL_JOIN;
    COMPARE;
    IS_NULL;
    IS_NOT_NULL;
    IN_LIST;
    SIMPLE_CASE;
    SEARCHED_CASE;
    FUNCTION_CALL;
    NEGATIVE;
    QNAME;
    CREATE_TABLE;
    TABLE_ELEMENT_LIST;
    COLUMN_DEF;
    NOT_NULL;
}

@header {
    package com.facebook.presto.sql.parser;
}

@lexer::header {
    package com.facebook.presto.sql.parser;
}

@members {
    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int tokenType, BitSet follow)
            throws RecognitionException
    {
        throw new MismatchedTokenException(tokenType, input);
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
            throws RecognitionException
    {
        throw e;
    }
}

@lexer::members {
    @Override
    public void reportError(RecognitionException e)
    {
        super.reportError(e);
        throw new RuntimeException(e);
    }
}

@rulecatch {
    catch (RecognitionException re) {
        reportError(re);
        throw re;
    }
}


singleStatement
    : statement EOF -> statement
    ;

statementList
    : (statement ';')* EOF -> ^(STATEMENT_LIST statement*)
    ;

statement
    : selectStmt      -> ^(QUERY selectStmt)
    | createTableStmt -> ^(CREATE_TABLE createTableStmt)
    ;

selectStmt
    : selectClause
      fromClause?
      whereClause?
      (groupClause havingClause?)?
      orderClause?
      limitClause?
    ;

selectClause
    : SELECT selectExpr -> ^(SELECT selectExpr)
    ;

fromClause
    : FROM tableRef (',' tableRef)* -> ^(FROM tableRef+)
    ;

whereClause
    : WHERE expr -> ^(WHERE expr)
    ;

groupClause
    : GROUP BY expr (',' expr)* -> ^(GROUPBY expr+)
    ;

havingClause
    : HAVING expr -> ^(HAVING expr)
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)* -> ^(ORDERBY sortItem+)
    ;

limitClause
    : LIMIT integer -> ^(LIMIT integer)
    ;

selectExpr
    : setQuant? selectList
    ;

setQuant
    : DISTINCT
    | ALL ->
    ;

selectList
    : '*' -> ALL_COLUMNS
    | selectSublist (',' selectSublist)* -> ^(SELECT_LIST selectSublist+)
    ;

selectSublist
    : expr (AS? ident)? -> ^(SELECT_ITEM expr ident?)
    | qname '.' '*'     -> ^(ALL_COLUMNS qname)
    ;

tableRef
    : ( tablePrimary -> tablePrimary )
      ( CROSS JOIN tablePrimary             -> ^(CROSS_JOIN $tableRef tablePrimary)
      | joinType JOIN tablePrimary joinSpec -> ^(QUALIFIED_JOIN joinType joinSpec $tableRef tablePrimary)
      | NATURAL joinType JOIN tablePrimary  -> ^(QUALIFIED_JOIN joinType NATURAL $tableRef tablePrimary)
      )*
    ;

tablePrimary
    : qname corrSpec?            -> ^(TABLE qname corrSpec?)
    | subquery corrSpec          -> ^(SUBQUERY subquery corrSpec)
    | '(' tableRef ')' corrSpec? -> ^(JOINED_TABLE tableRef corrSpec?)
    ;

joinType
    : INNER?       -> INNER_JOIN
    | LEFT OUTER?  -> LEFT_JOIN
    | RIGHT OUTER? -> RIGHT_JOIN
    | FULL OUTER?  -> FULL_JOIN
    ;

joinSpec
    : ON expr                          -> ^(ON expr)
    | USING '(' ident (',' ident)* ')' -> ^(USING ident+)
    ;

corrSpec
    : AS? ident corrList? -> ^(CORR_SPEC ident corrList?)
    ;

corrList
    : '(' ident (',' ident)* ')' -> ^(CORR_LIST ident+)
    ;

expr
    : orExpression
    ;

orExpression
    : andExpression (OR^ andExpression)*
    ;

andExpression
    : notExpression (AND^ notExpression)*
    ;

notExpression
    : (NOT^)* booleanTest
    ;

booleanTest
    : booleanPrimary
    ;

booleanPrimary
    : predicate
    | EXISTS subquery -> ^(EXISTS subquery)
    ;

predicate
    : (numericExpr -> numericExpr)
      ( cmpOp e=numericExpr                             -> ^(cmpOp $predicate $e)
      | BETWEEN min=numericExpr AND max=numericExpr     -> ^(BETWEEN $predicate $min $max)
      | NOT BETWEEN min=numericExpr AND max=numericExpr -> ^(NOT ^(BETWEEN $predicate $min $max))
      | LIKE e=numericExpr (ESCAPE x=numericExpr)?      -> ^(LIKE $predicate $e $x?)
      | NOT LIKE e=numericExpr (ESCAPE x=numericExpr)?  -> ^(NOT ^(LIKE $predicate $e $x?))
      | IS NULL                                         -> ^(IS_NULL $predicate)
      | IS NOT NULL                                     -> ^(IS_NOT_NULL $predicate)
      | IN inList                                       -> ^(IN $predicate inList)
      | NOT IN inList                                   -> ^(NOT ^(IN $predicate inList))
      )*
    ;

numericExpr
    : numericTerm (('+' | '-')^ numericTerm)*
    ;

numericTerm
    : numericFactor (('*' | '/' | '%')^ numericFactor)*
    ;

numericFactor
    : '+'? exprPrimary -> exprPrimary
    | '-' exprPrimary  -> ^(NEGATIVE exprPrimary)
    ;

exprPrimary
    : NULL
    | qname
    | function
    | specialFunction
    | number
    | STRING
    | dateValue
    | intervalValue
    | caseExpression
    | subquery
    | '(' expr ')' -> expr
    ;

inList
    : '(' expr (',' expr)* ')' -> ^(IN_LIST expr+)
    | subquery
    ;

sortItem
    : expr ordering nullOrdering? -> ^(SORT_ITEM expr ordering nullOrdering?)
    ;

ordering
    : -> ASC
    | ASC
    | DESC
    ;

nullOrdering
    : NULLS FIRST -> FIRST
    | NULLS LAST  -> LAST
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

subquery
    : '(' selectStmt ')' -> ^(QUERY selectStmt)
    ;

dateValue
    : DATE STRING      -> ^(DATE STRING)
    | TIME STRING      -> ^(TIME STRING)
    | TIMESTAMP STRING -> ^(TIMESTAMP STRING)
    ;

intervalValue
    : INTERVAL intervalSign? STRING intervalQualifier -> ^(INTERVAL STRING intervalQualifier intervalSign?)
    ;

intervalSign
    : '+' ->
    | '-' -> NEGATIVE
    ;

intervalQualifier
    : nonSecond ('(' integer ')')?                 -> ^(nonSecond integer?)
    | SECOND ('(' p=integer (',' s=integer)? ')')? -> ^(SECOND $p? $s?)
    ;

nonSecond
    : YEAR | MONTH | DAY | HOUR | MINUTE
    ;

specialFunction
    : CURRENT_DATE                                 -> ^(FUNCTION_CALL CURRENT_DATE)
    | CURRENT_TIME ('(' integer ')')?              -> ^(FUNCTION_CALL CURRENT_TIME integer?)
    | CURRENT_TIMESTAMP ('(' integer ')')?         -> ^(FUNCTION_CALL CURRENT_TIMESTAMP integer?)
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')' -> ^(FUNCTION_CALL SUBSTRING expr expr expr?)
    | EXTRACT '(' extractField FROM expr ')'       -> ^(FUNCTION_CALL EXTRACT extractField expr)
    ;

extractField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | TIMEZONE_HOUR | TIMEZONE_MINUTE
    ;

caseExpression
    : NULLIF '(' expr ',' expr ')'          -> ^(NULLIF expr expr)
    | COALESCE '(' expr (',' expr)* ')'     -> ^(COALESCE expr+)
    | CASE expr whenClause+ elseClause? END -> ^(SIMPLE_CASE expr whenClause+ elseClause?)
    | CASE whenClause+ elseClause? END      -> ^(SEARCHED_CASE whenClause+ elseClause?)
    ;

whenClause
    : WHEN expr THEN expr -> ^(WHEN expr expr)
    ;

elseClause
    : ELSE expr -> expr
    ;

function
    : qname '(' '*' ')'                         -> ^(FUNCTION_CALL qname)
    | qname '(' setQuant? expr? (',' expr)* ')' -> ^(FUNCTION_CALL qname setQuant? expr*)
    ;

createTableStmt
    : CREATE TABLE qname tableElementList -> qname tableElementList
    ;

tableElementList
    : '(' tableElement (',' tableElement)* ')' -> ^(TABLE_ELEMENT_LIST tableElement+)
    ;

tableElement
    : ident dataType columnConstDef* -> ^(COLUMN_DEF ident dataType columnConstDef*)
    ;

dataType
    : charType
    | exactNumType
    | dateType
    ;

charType
    : CHAR charlen?              -> ^(CHAR charlen?)
    | CHARACTER charlen?         -> ^(CHAR charlen?)
    | VARCHAR charlen?           -> ^(VARCHAR charlen?)
    | CHAR VARYING charlen?      -> ^(VARCHAR charlen?)
    | CHARACTER VARYING charlen? -> ^(VARCHAR charlen?)
    ;

charlen
    : '(' integer ')' -> integer
    ;

exactNumType
    : NUMERIC numlen? -> ^(NUMERIC numlen?)
    | DECIMAL numlen? -> ^(NUMERIC numlen?)
    | DEC numlen?     -> ^(NUMERIC numlen?)
    | INTEGER         -> ^(INTEGER)
    | INT             -> ^(INTEGER)
    ;

numlen
    : '(' p=integer (',' s=integer)? ')' -> $p $s?
    ;

dateType
    : DATE -> ^(DATE)
    ;

columnConstDef
    : columnConst -> ^(CONSTRAINT columnConst)
    ;

columnConst
    : NOT NULL -> NOT_NULL
    ;

qname
    : ident ('.' ident)* -> ^(QNAME ident+)
    ;

ident
    : IDENT
    ;

number
    : DECIMAL_VALUE
    | INTEGER_VALUE
    ;

integer
    : INTEGER_VALUE
    ;

SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
ALL: 'ALL';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
IS: 'IS';
NULL: 'NULL';
NULLS: 'NULLS';
FIRST: 'FIRST';
LAST: 'LAST';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
FOR: 'FOR';
DATE: 'DATE';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
INTERVAL: 'INTERVAL';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
EXTRACT: 'EXTRACT';
TIMEZONE_HOUR: 'TIMEZONE_HOUR';
TIMEZONE_MINUTE: 'TIMEZONE_MINUTE';
COALESCE: 'COALESCE';
NULLIF: 'NULLIF';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
CREATE: 'CREATE';
TABLE: 'TABLE';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
VARYING: 'VARYING';
VARCHAR: 'VARCHAR';
NUMERIC: 'NUMERIC';
NUMBER: 'NUMBER';
DECIMAL: 'DECIMAL';
DEC: 'DEC';
INTEGER: 'INTEGER';
INT: 'INT';
CONSTRAINT: 'CONSTRAINT';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

STRING
    : '\'' ( ~'\'' | '\'' '\'' )* '\''
        { setText(getText().substring(1, getText().length() - 1)); }
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

IDENT
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : 'A'..'Z'
    ;

COMMENT
    : '--' (~('\r' | '\n'))* ('\r'? '\n')?     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel=HIDDEN; }
    ;
