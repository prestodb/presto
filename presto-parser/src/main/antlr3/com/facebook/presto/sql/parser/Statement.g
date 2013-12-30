/*
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
    memoize=true;
}

tokens {
    LEXER_ERROR;
    TERMINATOR;
    STATEMENT_LIST;
    GROUP_BY;
    ORDER_BY;
    SORT_ITEM;
    QUERY;
    WITH_LIST;
    WITH_QUERY;
    ALL_COLUMNS;
    SELECT_LIST;
    SELECT_ITEM;
    ALIASED_COLUMNS;
    TABLE_SUBQUERY;
    EXPLAIN_OPTIONS;
    EXPLAIN_FORMAT;
    EXPLAIN_TYPE;
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
    IS_DISTINCT_FROM;
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
    SHOW_SCHEMAS;
    SHOW_CATALOGS;
    SHOW_COLUMNS;
    SHOW_PARTITIONS;
    SHOW_FUNCTIONS;
    CREATE_TABLE;
    CREATE_MATERIALIZED_VIEW;
    REFRESH_MATERIALIZED_VIEW;
    VIEW_REFRESH;
    CREATE_ALIAS;
    DROP_ALIAS;
    DROP_TABLE;
    TABLE_ELEMENT_LIST;
    COLUMN_DEF;
    NOT_NULL;
    ALIASED_RELATION;
    SAMPLED_RELATION;
    QUERY_SPEC;
    STRATIFY_ON;
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
        if (e.token.getType() == DIGIT_IDENT) {
            return "identifiers must not start with a digit; surround the identifier with double quotes";
        }
        if (e.token.getType() == COLON_IDENT) {
            return "identifiers must not contain a colon; use '@' instead of ':' for table links";
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

singleExpression
    : expr EOF -> expr
    ;

statement
    : query
    | explainStmt
    | showTablesStmt
    | showSchemasStmt
    | showCatalogsStmt
    | showColumnsStmt
    | showPartitionsStmt
    | showFunctionsStmt
    | createTableStmt
    | dropTableStmt
    | createMaterializedViewStmt
    | refreshMaterializedViewStmt
    | createAliasStmt
    | dropAliasStmt
    ;

query
    : queryExpr -> ^(QUERY queryExpr)
    ;

queryExpr
    : withClause?
      ( (orderOrLimitQuerySpec) => orderOrLimitQuerySpec
      | queryExprBody orderClause? limitClause?
      )
    ;

orderOrLimitQuerySpec
    : simpleQuery (orderClause limitClause? | limitClause) -> ^(QUERY_SPEC simpleQuery orderClause? limitClause?)
    ;

queryExprBody
    : ( queryTerm -> queryTerm )
      ( UNION setQuant? queryTerm       -> ^(UNION $queryExprBody queryTerm setQuant?)
      | EXCEPT setQuant? queryTerm      -> ^(EXCEPT $queryExprBody queryTerm setQuant?)
      )*
    ;

queryTerm
    : ( queryPrimary -> queryPrimary )
      ( INTERSECT setQuant? queryPrimary -> ^(INTERSECT $queryTerm queryPrimary setQuant?) )*
    ;

queryPrimary
    : simpleQuery -> ^(QUERY_SPEC simpleQuery)
    | tableSubquery
    | explicitTable
    ;

explicitTable
    : TABLE table -> table
    ;

simpleQuery
    : selectClause
      fromClause?
      whereClause?
      groupClause?
      havingClause?
    ;

restrictedSelectStmt
    : selectClause
      fromClause
    ;

withClause
    : WITH r=RECURSIVE? withList -> ^(WITH $r? withList)
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

withList
    : withQuery (',' withQuery)* -> ^(WITH_LIST withQuery+)
    ;

withQuery
    : ident aliasedColumns? AS subquery -> ^(WITH_QUERY ident subquery aliasedColumns?)
    ;

selectExpr
    : setQuant? selectList
    ;

setQuant
    : DISTINCT
    | ALL
    ;

selectList
    : selectSublist (',' selectSublist)* -> ^(SELECT_LIST selectSublist+)
    ;

selectSublist
    : expr (AS? ident)? -> ^(SELECT_ITEM expr ident?)
    | qname '.' '*'     -> ^(ALL_COLUMNS qname)
    | '*'               -> ALL_COLUMNS
    ;

tableRef
    : ( tableFactor -> tableFactor )
      ( CROSS JOIN tableFactor                 -> ^(CROSS_JOIN $tableRef tableFactor)
      | joinType JOIN tableFactor joinCriteria -> ^(QUALIFIED_JOIN joinType joinCriteria $tableRef tableFactor)
      | NATURAL joinType JOIN tableFactor      -> ^(QUALIFIED_JOIN joinType NATURAL $tableRef tableFactor)
      )*
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

stratifyOn
    : STRATIFY ON '(' expr (',' expr)* ')' -> ^(STRATIFY_ON expr+)
    ;

tableFactor
    : ( tablePrimary -> tablePrimary )
      ( TABLESAMPLE sampleType '(' expr ')' stratifyOn? -> ^(SAMPLED_RELATION $tableFactor sampleType expr stratifyOn?) )?
    ;

tablePrimary
    : ( relation -> relation )
      ( AS? ident aliasedColumns? -> ^(ALIASED_RELATION $tablePrimary ident aliasedColumns?) )?
    ;

relation
    : table
    | ('(' tableRef ')') => joinedTable
    | tableSubquery
    ;

table
    : qname -> ^(TABLE qname)
    ;

tableSubquery
    : '(' query ')' -> ^(TABLE_SUBQUERY query)
    ;

joinedTable
    : '(' tableRef ')' -> ^(JOINED_TABLE tableRef)
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
      | IS DISTINCT FROM e=predicatePrimary                       -> ^(IS_DISTINCT_FROM $predicate $e)
      | IS NOT DISTINCT FROM e=predicatePrimary                   -> ^(NOT ^(IS_DISTINCT_FROM $predicate $e))
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
    | (dateValue) => dateValue
    | (intervalValue) => intervalValue
    | qnameOrFunction
    | specialFunction
    | number
    | bool
    | STRING
    | caseExpression
    | ('(' expr ')') => ('(' expr ')' -> expr)
    | subquery
    ;

qnameOrFunction
    : (qname -> qname)
      ( ('(' '*' ')' over?                          -> ^(FUNCTION_CALL $qnameOrFunction over?))
      | ('(' setQuant? expr? (',' expr)* ')' over?  -> ^(FUNCTION_CALL $qnameOrFunction over? setQuant? expr*))
      )?
    ;

inList
    : ('(' expr) => ('(' expr (',' expr)* ')' -> ^(IN_LIST expr+))
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
    : '(' query ')' -> query
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
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')' -> ^(FUNCTION_CALL ^(QNAME IDENT["substr"]) expr expr expr?)
    | EXTRACT '(' ident FROM expr ')'              -> ^(EXTRACT ident expr)
    | CAST '(' expr AS type ')'                    -> ^(CAST expr IDENT[$type.text])
    ;

// TODO: this should be 'dataType', which supports arbitrary type specifications. For now we constrain to simple types
type
    : VARCHAR
    | BIGINT
    | DOUBLE
    | BOOLEAN
    ;

caseExpression
    : NULLIF '(' expr ',' expr ')'          -> ^(NULLIF expr expr)
    | COALESCE '(' expr (',' expr)* ')'     -> ^(COALESCE expr+)
    | CASE expr whenClause+ elseClause? END -> ^(SIMPLE_CASE expr whenClause+ elseClause?)
    | CASE whenClause+ elseClause? END      -> ^(SEARCHED_CASE whenClause+ elseClause?)
    | IF '(' expr ',' expr (',' expr)? ')'  -> ^(IF expr expr expr?)
    ;

whenClause
    : WHEN expr THEN expr -> ^(WHEN expr expr)
    ;

elseClause
    : ELSE expr -> expr
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

explainStmt
    : EXPLAIN explainOptions? statement -> ^(EXPLAIN explainOptions? statement)
    ;

explainOptions
    : '(' explainOption (',' explainOption)* ')' -> ^(EXPLAIN_OPTIONS explainOption+)
    ;

explainOption
    : FORMAT TEXT      -> ^(EXPLAIN_FORMAT TEXT)
    | FORMAT GRAPHVIZ  -> ^(EXPLAIN_FORMAT GRAPHVIZ)
    | TYPE LOGICAL     -> ^(EXPLAIN_TYPE LOGICAL)
    | TYPE DISTRIBUTED -> ^(EXPLAIN_TYPE DISTRIBUTED)
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

showSchemasStmt
    : SHOW SCHEMAS from=showSchemasFrom? -> ^(SHOW_SCHEMAS $from?)
    ;

showSchemasFrom
    : (FROM | IN) ident -> ^(FROM ident)
    ;

showCatalogsStmt
    : SHOW CATALOGS -> SHOW_CATALOGS
    ;

showColumnsStmt
    : SHOW COLUMNS (FROM | IN) qname -> ^(SHOW_COLUMNS qname)
    | DESCRIBE qname                 -> ^(SHOW_COLUMNS qname)
    | DESC qname                     -> ^(SHOW_COLUMNS qname)
    ;

showPartitionsStmt
    : SHOW PARTITIONS (FROM | IN) qname w=whereClause? o=orderClause? l=limitClause? -> ^(SHOW_PARTITIONS qname $w? $o? $l?)
    ;

showFunctionsStmt
    : SHOW FUNCTIONS -> SHOW_FUNCTIONS
    ;

dropTableStmt
    : DROP TABLE qname -> ^(DROP_TABLE qname)
    ;

createMaterializedViewStmt
    : CREATE MATERIALIZED VIEW qname r=viewRefresh? AS s=restrictedSelectStmt -> ^(CREATE_MATERIALIZED_VIEW qname $r? $s)
    ;

refreshMaterializedViewStmt
    : REFRESH MATERIALIZED VIEW qname -> ^(REFRESH_MATERIALIZED_VIEW qname)
    ;

viewRefresh
    : REFRESH r=integer -> ^(REFRESH $r)
    ;

createAliasStmt
    : CREATE ALIAS qname forRemote -> ^(CREATE_ALIAS qname forRemote)
    ;

dropAliasStmt
    : DROP ALIAS qname -> ^(DROP_ALIAS qname)
    ;

forRemote
    : FOR qname -> ^(FOR qname)
    ;

createTableStmt
    : CREATE TABLE qname s=tableContentsSource -> ^(CREATE_TABLE qname $s)
    ;

tableContentsSource
    : AS query -> query
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
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW
    | REFRESH | MATERIALIZED | VIEW | ALIAS
    | DATE | TIME | TIMESTAMP | INTERVAL
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI
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
COALESCE: 'COALESCE';
NULLIF: 'NULLIF';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
IF: 'IF';
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
WITH: 'WITH';
RECURSIVE: 'RECURSIVE';
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
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
GRAPHVIZ: 'GRAPHVIZ';
LOGICAL: 'LOGICAL';
DISTRIBUTED: 'DISTRIBUTED';
CAST: 'CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
SCHEMAS: 'SCHEMAS';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
REFRESH: 'REFRESH';
DROP: 'DROP';
ALIAS: 'ALIAS';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
SYSTEM: 'SYSTEM';
BERNOULLI: 'BERNOULLI';
TABLESAMPLE: 'TABLESAMPLE';
STRATIFY: 'STRATIFY';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

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

DIGIT_IDENT
    : DIGIT (LETTER | DIGIT | '_' | '\@')+
    ;

QUOTED_IDENT
    : '"' ( ~'"' | '""' )* '"'
        { setText(getText().substring(1, getText().length() - 1).replace("\"\"", "\"")); }
    ;

BACKQUOTED_IDENT
    : '`' ( ~'`' | '``' )* '`'
        { setText(getText().substring(1, getText().length() - 1).replace("``", "`")); }
    ;

COLON_IDENT
    : (LETTER | DIGIT | '_' )+ ':' (LETTER | DIGIT | '_' )+
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
