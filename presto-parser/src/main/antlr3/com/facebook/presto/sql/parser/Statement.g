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
    LEXER_ERROR;
    STATEMENT_LIST;
    GROUP_BY;
    ORDER_BY;
    SORT_ITEM;
    QUERY;
    ALL_COLUMNS;
    SELECT_LIST;
    SELECT_ITEM;
    TABLE_ALIAS;
    ALIASED_COLUMNS;
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
    WINDOW;
    PARTITION_BY;
    UNBOUNDED_PRECEDING;
    UNBOUNDED_FOLLOWING;
    CURRENT_ROW;
    NEGATIVE;
    QNAME;
    SHOW_TABLES;
    SHOW_COLUMNS;
    SHOW_PARTITIONS;
    SHOW_FUNCTIONS;
    CREATE_TABLE;
    CREATE_MATERIALIZED_VIEW;
    REFRESH_MATERIALIZED_VIEW;
    DROP_TABLE;
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

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames)
    {
        if (e.token.getType() == BACKQUOTED_IDENT) {
            return "backquoted identifiers are not supported; use double quotes to quote identifiers";
        }
        return super.getErrorMessage(e, tokenNames);
    }
}

@lexer::members {
    @Override
    public void reportError(RecognitionException e)
    {
        throw new ParsingException(getErrorMessage(e, getTokenNames()), e);
    }
}

@rulecatch {
    catch (RecognitionException re) {
        throw new ParsingException(getErrorMessage(re, getTokenNames()), re);
    }
}


singleStatement
    : statement EOF -> statement
    ;

statementList
    : (statement SEMICOLON)* EOF -> ^(STATEMENT_LIST statement*)
    ;

singleExpression
    : expr EOF -> expr
    ;

statement
    : selectStmt      -> ^(QUERY selectStmt)
    | showTablesStmt
    | showColumnsStmt
    | showPartitionsStmt
    | showFunctionsStmt
    | createTableStmt
    | createMaterializedViewStmt
    | refreshMaterializedViewStmt
    | dropTableStmt
    ;

selectStmt
    : selectClause
      fromClause?
      whereClause?
      (groupClause havingClause?)?
      orderClause?
      limitClause?
    ;

restrictedSelectStmt
    : selectClause
      fromClause
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
    : GROUP BY expr (',' expr)* -> ^(GROUP_BY expr+)
    ;

havingClause
    : HAVING expr -> ^(HAVING expr)
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)* -> ^(ORDER_BY sortItem+)
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
      ( CROSS JOIN tablePrimary                 -> ^(CROSS_JOIN $tableRef tablePrimary)
      | joinType JOIN tablePrimary joinCriteria -> ^(QUALIFIED_JOIN joinType joinCriteria $tableRef tablePrimary)
      | NATURAL joinType JOIN tablePrimary      -> ^(QUALIFIED_JOIN joinType NATURAL $tableRef tablePrimary)
      )*
    ;

tablePrimary
    : qname tableAlias?            -> ^(TABLE qname tableAlias?)
    | subquery tableAlias          -> ^(SUBQUERY subquery tableAlias)
    | '(' tableRef ')' tableAlias? -> ^(JOINED_TABLE tableRef tableAlias?)
    ;

joinType
    : INNER?       -> INNER_JOIN
    | LEFT OUTER?  -> LEFT_JOIN
    | RIGHT OUTER? -> RIGHT_JOIN
    | FULL OUTER?  -> FULL_JOIN
    ;

joinCriteria
    : ON expr                          -> ^(ON expr)
    | USING '(' ident (',' ident)* ')' -> ^(USING ident+)
    ;

tableAlias
    : AS? ident aliasedColumns? -> ^(TABLE_ALIAS ident aliasedColumns?)
    ;

aliasedColumns
    : '(' ident (',' ident)* ')' -> ^(ALIASED_COLUMNS ident+)
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
    : (predicatePrimary -> predicatePrimary)
      ( cmpOp e=predicatePrimary                                  -> ^(cmpOp $predicate $e)
      | BETWEEN min=predicatePrimary AND max=predicatePrimary     -> ^(BETWEEN $predicate $min $max)
      | NOT BETWEEN min=predicatePrimary AND max=predicatePrimary -> ^(NOT ^(BETWEEN $predicate $min $max))
      | LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?      -> ^(LIKE $predicate $e $x?)
      | NOT LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?  -> ^(NOT ^(LIKE $predicate $e $x?))
      | IS NULL                                                   -> ^(IS_NULL $predicate)
      | IS NOT NULL                                               -> ^(IS_NOT_NULL $predicate)
      | IN inList                                                 -> ^(IN $predicate inList)
      | NOT IN inList                                             -> ^(NOT ^(IN $predicate inList))
      )*
    ;

predicatePrimary
    : (numericExpr -> numericExpr)
      ( '||' e=numericExpr -> ^(FUNCTION_CALL ^(QNAME IDENT["concat"]) $predicatePrimary $e) )*
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
    | bool
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
    : CURRENT_DATE
    | CURRENT_TIME ('(' integer ')')?              -> ^(CURRENT_TIME integer?)
    | CURRENT_TIMESTAMP ('(' integer ')')?         -> ^(CURRENT_TIMESTAMP integer?)
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')' -> ^(FUNCTION_CALL ^(QNAME IDENT[$SUBSTRING]) expr expr expr?)
    | EXTRACT '(' extractFieldOrIdent FROM expr ')'-> ^(EXTRACT IDENT[$extractFieldOrIdent.text] expr)
    // handle function call-like syntax for extract
    | extractField '(' expr ')'                    -> ^(FUNCTION_CALL ^(QNAME IDENT[$extractField.text]) expr)
    | CAST '(' expr AS type ')'                    -> ^(CAST expr IDENT[$type.text])
    ;

// TODO: this should be 'dataType', which supports arbitrary type specifications. For now we constrain to simple types
type
    : VARCHAR
    | BIGINT
    | DOUBLE
    | BOOLEAN
    ;

extractFieldOrIdent
    : ident
    | extractField
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
    : qname '(' '*' ')' over?                         -> ^(FUNCTION_CALL qname over?)
    | qname '(' setQuant? expr? (',' expr)* ')' over? -> ^(FUNCTION_CALL qname over? setQuant? expr*)
    ;

over
    : OVER '(' window ')' -> window
    ;

window
    : p=windowPartition? o=orderClause? f=windowFrame? -> ^(WINDOW $p? $o ?$f?)
    ;

windowPartition
    : PARTITION BY expr (',' expr)* -> ^(PARTITION_BY expr+)
    ;

windowFrame
    : RANGE frameBound                        -> ^(RANGE frameBound)
    | ROWS frameBound                         -> ^(ROWS frameBound)
    | RANGE BETWEEN frameBound AND frameBound -> ^(RANGE frameBound frameBound)
    | ROWS BETWEEN frameBound AND frameBound  -> ^(ROWS frameBound frameBound)
    ;

frameBound
    : UNBOUNDED PRECEDING -> UNBOUNDED_PRECEDING
    | UNBOUNDED FOLLOWING -> UNBOUNDED_FOLLOWING
    | CURRENT ROW         -> CURRENT_ROW
    | expr
      ( PRECEDING -> ^(PRECEDING expr)
      | FOLLOWING -> ^(FOLLOWING expr)
      )
    ;

showTablesStmt
    : SHOW TABLES from=showTablesFrom? like=showTablesLike? -> ^(SHOW_TABLES $from? $like?)
    ;

showTablesFrom
    : (FROM | IN) qname -> ^(FROM qname)
    ;

showTablesLike
    : LIKE s=STRING -> ^(LIKE $s)
    ;

showColumnsStmt
    : SHOW COLUMNS (FROM | IN) qname -> ^(SHOW_COLUMNS qname)
    | DESCRIBE qname                 -> ^(SHOW_COLUMNS qname)
    | DESC qname                     -> ^(SHOW_COLUMNS qname)
    ;

showPartitionsStmt
    : SHOW PARTITIONS (FROM | IN) qname -> ^(SHOW_PARTITIONS qname)
    ;

showFunctionsStmt
    : SHOW FUNCTIONS -> SHOW_FUNCTIONS
    ;

dropTableStmt
    : DROP TABLE qname -> ^(DROP_TABLE qname)
    ;

createMaterializedViewStmt
    : CREATE MATERIALIZED VIEW qname refresh=viewRefresh? AS restrictedSelectStmt -> ^(CREATE_MATERIALIZED_VIEW qname $refresh? restrictedSelectStmt)
    ;

refreshMaterializedViewStmt
    : REFRESH MATERIALIZED VIEW qname -> ^(REFRESH_MATERIALIZED_VIEW qname)
    ;

viewRefresh
    : REFRESH r=integer -> ^(REFRESH $r)
    ;

createTableStmt
    : CREATE TABLE qname tableElementList -> ^(CREATE_TABLE qname tableElementList)
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
    | QUOTED_IDENT
    | nonReserved  -> IDENT[$nonReserved.text]
    ;

number
    : DECIMAL_VALUE
    | INTEGER_VALUE
    ;

bool
    : TRUE
    | FALSE
    ;

integer
    : INTEGER_VALUE
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW
    | REFRESH | MATERIALIZED | VIEW
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
TRUE: 'TRUE';
FALSE: 'FALSE';
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
OVER: 'OVER';
PARTITION: 'PARTITION';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
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
DOUBLE: 'DOUBLE';
BIGINT: 'BIGINT';
BOOLEAN: 'BOOLEAN';
CONSTRAINT: 'CONSTRAINT';
DESCRIBE: 'DESCRIBE';
CAST: 'CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
COLUMNS: 'COLUMNS';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
REFRESH: 'REFRESH';
DROP: 'DROP';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

SEMICOLON: ';';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
        { setText(getText().substring(1, getText().length() - 1).replace("''", "'")); }
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENT
    : (LETTER | '_') (LETTER | DIGIT | '_' | '\@')*
    ;

QUOTED_IDENT
    : '"' ( ~'"' | '""' )* '"'
        { setText(getText().substring(1, getText().length() - 1).replace("\"\"", "\"")); }
    ;

BACKQUOTED_IDENT
    : '`' ( ~'`' | '``' )* '`'
        { setText(getText().substring(1, getText().length() - 1).replace("``", "`")); }
    ;

fragment EXPONENT
    : 'E' ('+' | '-')? DIGIT+
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
