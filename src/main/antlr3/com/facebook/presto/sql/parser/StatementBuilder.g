tree grammar StatementBuilder;

options {
    tokenVocab = Statement;
    output = AST;
    ASTLabelType = CommonTree;
}

@header {
    package com.facebook.presto.sql.parser;

    import com.facebook.presto.sql.tree.*;

    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;
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

@rulecatch {
    catch (RecognitionException re) {
        reportError(re);
        throw re;
    }
}


statement returns [Statement value]
    : query        { $value = $query.value; }
    ;

query returns [Query value]
    : ^(QUERY selectStmt) { $value = $selectStmt.value; }
    ;

selectStmt returns [Query value]
    : s=selectClause
      f=fromClause?
      w=whereClause?
      (g=groupClause h=havingClause?)?
      o=orderClause?
      l=limitClause?
        { $value = new Query($s.value, $f.value, $w.value, $g.value, $h.value, $o.value, $l.value); }
    ;


selectClause returns [Select value]
    : ^(SELECT d=distinct ALL_COLUMNS)  { $value = new Select($d.value); }
    | ^(SELECT d=distinct s=selectList) { $value = new Select($d.value, $s.value); }
    ;

distinct returns [boolean value]
    : DISTINCT { $value = true; }
    |          { $value = false; }
    ;

selectList returns [List<SelectItem> value = new ArrayList<>()]
    : ^(SELECT_LIST ( selectItem { $value.add($selectItem.value); } )+ )
    ;

selectItem returns [SelectItem value]
    : ^(SELECT_ITEM e=expr i=ident?) { $value = new SelectItemExpression($e.value, $i.value); }
    | ^(ALL_COLUMNS q=qname)         { $value = new SelectItemAllColumns($q.value); }
    ;

fromClause returns [List<TableReference> value]
    : ^(FROM t=tableList) { $value = $t.value; }
    ;

whereClause returns [Expression value]
    : ^(WHERE expr) { $value = $expr.value; }
    ;

groupClause returns [List<Expression> value]
    : ^(GROUPBY exprList) { $value = $exprList.value; }
    ;

havingClause returns [Expression value]
    : ^(HAVING expr) { $value = $expr.value; }
    ;

orderClause returns [List<SortItem> value = new ArrayList<>()]
    : ^(ORDERBY ( sortItem { $value.add($sortItem.value); } )+ )
    ;

sortItem returns [SortItem value]
    : ^(SORT_ITEM expr o=ordering n=nullOrdering) { $value = new SortItem($expr.value, $o.value, $n.value); }
    ;

ordering returns [SortItem.Ordering value]
    : ASC  { $value = SortItem.Ordering.ASCENDING; }
    | DESC { $value = SortItem.Ordering.DESCENDING; }
    ;

nullOrdering returns [SortItem.NullOrdering value]
    : FIRST { $value = SortItem.NullOrdering.FIRST; }
    | LAST  { $value = SortItem.NullOrdering.LAST; }
    |       { $value = SortItem.NullOrdering.UNDEFINED; }
    ;

limitClause returns [String value]
    : ^(LIMIT integer) { $value = $integer.value; }
    ;

tableList returns [List<TableReference> value = new ArrayList<>()]
    : ( t=tableRef { $value.add($t.value); } )+
    ;

tableRef returns [TableReference value]
    : p=tablePrimary { $value = $p.value; }
    ;

tablePrimary returns [TablePrimary value]
    : ^(TABLE n=qname c=corrSpec?)           { $value = new NamedTable($n.value, $c.value); }
    | ^(SUBQUERY q=subquery c=corrSpec)      { $value = new SubqueryTable($q.value, $c.value); }
    | ^(JOINED_TABLE t=tableRef c=corrSpec?) { $value = new JoinedTable($t.value, $c.value); }
    ;

corrSpec returns [TableCorrelation value]
    : ^(CORR_SPEC i=ident) { $value = new TableCorrelation($i.value); }
    ;

subquery returns [Query value]
    : ^(QUERY s=selectStmt) { $value = $s.value; }
    ;

expr returns [Expression value]
    : NULL                  { $value = new NullLiteral(); }
    | qname                 { $value = new QualifiedNameReference($qname.value); }
    | functionCall          { $value = $functionCall.value; }
    | arithmeticExpression  { $value = $arithmeticExpression.value; }
    | comparisonExpression  { $value = $comparisonExpression.value; }
    | ^(AND a=expr b=expr)  { $value = LogicalBinaryExpression.and($a.value, $b.value); }
    | ^(OR a=expr b=expr)   { $value = LogicalBinaryExpression.or($a.value, $b.value); }
    | ^(NOT e=expr)         { $value = new NotExpression($e.value); }
    | ^(DATE string)        { $value = new DateLiteral($string.value); }
    | ^(TIME string)        { $value = new TimeLiteral($string.value); }
    | ^(TIMESTAMP string)   { $value = new TimestampLiteral($string.value); }
    | string                { $value = new StringLiteral($string.value); }
    | integer               { $value = new LongLiteral($integer.value); }
    | decimal               { $value = new DoubleLiteral($decimal.value); }
    | intervalValue         { $value = $intervalValue.value; }
    | predicate             { $value = $predicate.value; }
    | ^(IN_LIST exprList)   { $value = new InListExpression($exprList.value); }
    | ^(NEGATIVE e=expr)    { $value = new NegativeExpression($e.value); }
    | caseExpression        { $value = $caseExpression.value; }
    | subquery              { $value = new SubqueryExpression($subquery.value); }
    ;

exprList returns [List<Expression> value = new ArrayList<>()]
    : ( expr { $value.add($expr.value); } )*
    ;

qname returns [QualifiedName value]
    : ^(QNAME i=identList) { $value = new QualifiedName($i.value); }
    ;

identList returns [List<String> value = new ArrayList<>()]
    : ( ident { $value.add($ident.value); } )+
    ;

ident returns [String value]
    : i=IDENT { $value = $i.text; }
    ;

string returns [String value]
    : s=STRING { $value = $s.text; }
    ;

integer returns [String value]
    : s=INTEGER_VALUE { $value = $s.text; }
    ;

decimal returns [String value]
    : s=DECIMAL_VALUE { $value = $s.text; }
    ;

functionCall returns [FunctionCall value]
    : ^(FUNCTION_CALL n=qname d=distinct a=exprList) { $value = new FunctionCall($n.value, $d.value, $a.value); }
    | ^(FUNCTION_CALL CURRENT_DATE)                  { $value = new FunctionCall("current_date"); }
    | ^(FUNCTION_CALL CURRENT_TIME a=exprList)       { $value = new FunctionCall("current_time", $a.value); }
    | ^(FUNCTION_CALL CURRENT_TIMESTAMP a=exprList)  { $value = new FunctionCall("current_timestamp", $a.value); }
    | ^(FUNCTION_CALL SUBSTRING a=exprList)          { $value = new FunctionCall("substr", $a.value); }
    | ^(FUNCTION_CALL EXTRACT e=extractArgs)         { $value = new FunctionCall("extract", $e.value); }
    ;

extractArgs returns [List<Expression> value = new ArrayList<>()]
    : extractField expr { $value.add($extractField.value); $value.add($expr.value); }
    ;

extractField returns [Expression value]
    : t=(YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | TIMEZONE_HOUR | TIMEZONE_MINUTE) { $value = new StringLiteral($t.text); }
    ;

arithmeticExpression returns [ArithmeticExpression value]
    : ^(t=arithmeticType a=expr b=expr) { $value = new ArithmeticExpression($t.value, $a.value, $b.value); }
    ;

arithmeticType returns [ArithmeticExpression.Type value]
    : '+' { $value = ArithmeticExpression.Type.ADD; }
    | '-' { $value = ArithmeticExpression.Type.SUBTRACT; }
    | '*' { $value = ArithmeticExpression.Type.MULTIPLY; }
    | '/' { $value = ArithmeticExpression.Type.DIVIDE; }
    | '%' { $value = ArithmeticExpression.Type.MODULUS; }
    ;

comparisonExpression returns [ComparisonExpression value]
    : ^(t=comparisonType a=expr b=expr) { $value = new ComparisonExpression($t.value, $a.value, $b.value); }
    ;

comparisonType returns [ComparisonExpression.Type value]
    : EQ  { $value = ComparisonExpression.Type.EQUAL; }
    | NEQ { $value = ComparisonExpression.Type.NOT_EQUAL; }
    | LT  { $value = ComparisonExpression.Type.LESS_THAN; }
    | LTE { $value = ComparisonExpression.Type.LESS_THAN_OR_EQUAL; }
    | GT  { $value = ComparisonExpression.Type.GREATER_THAN; }
    | GTE { $value = ComparisonExpression.Type.GREATER_THAN_OR_EQUAL; }
    ;

intervalValue returns [IntervalLiteral value]
    : ^(INTERVAL s=string q=intervalQualifier g=intervalSign) { $value = new IntervalLiteral($s.value, $q.value, $g.value); }
    ;

intervalQualifier returns [String value]
    : ^(t=nonSecond p=integer?)         { $value = String.format("\%s (\%s)", $t.value, $p.value); }
    | ^(SECOND (p=integer s=integer?)?) { $value = String.format("SECOND (\%s, \%s)", $p.value, $s.value); }
    ;

nonSecond returns [String value]
    : t=(YEAR | MONTH | DAY | HOUR | MINUTE) { $value = $t.text; }
    ;

intervalSign returns [IntervalLiteral.Sign value]
    : NEGATIVE { $value = IntervalLiteral.Sign.NEGATIVE; }
    |          { $value = IntervalLiteral.Sign.POSITIVE; }
    ;

predicate returns [Expression value]
    : ^(BETWEEN v=expr min=expr max=expr) { $value = new BetweenPredicate($v.value, $min.value, $max.value); }
    | ^(LIKE v=expr p=expr esc=expr?)     { $value = new LikePredicate($v.value, $p.value, $esc.value); }
    | ^(IS_NULL expr)                     { $value = new IsNullPredicate($expr.value); }
    | ^(IS_NOT_NULL expr)                 { $value = new IsNotNullPredicate($expr.value); }
    | ^(IN v=expr list=expr)              { $value = new InPredicate($v.value, $list.value); }
    | ^(EXISTS q=subquery)                { $value = new ExistsPredicate($q.value); }
    ;

caseExpression returns [Expression value]
    : ^(NULLIF a=expr b=expr)                { $value = new NullIfExpression($a.value, $b.value); }
    | ^(COALESCE exprList)                   { $value = new CoalesceExpression($exprList.value); }
    | ^(SIMPLE_CASE v=expr whenList e=expr?) { $value = new SimpleCaseExpression($v.value, $whenList.value, $e.value); }
    | ^(SEARCHED_CASE whenList e=expr?)      { $value = new SearchedCaseExpression($whenList.value, $e.value); }
    ;

whenList returns [List<WhenClause> value = new ArrayList<>()]
    : ( ^(WHEN a=expr b=expr) { $value.add(new WhenClause($a.value, $b.value)); } )+
    ;
