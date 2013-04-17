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
    import com.google.common.collect.ImmutableList;
    import com.google.common.base.Objects;
    import com.google.common.base.Optional;
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
        throw new IllegalArgumentException("bad tree from parser: " + getErrorMessage(re, getTokenNames()), re);
    }
}


statement returns [Statement value]
    : query           { $value = $query.value; }
    | showTables      { $value = $showTables.value; }
    | showColumns     { $value = $showColumns.value; }
    | showPartitions  { $value = $showPartitions.value; }
    | showFunctions   { $value = $showFunctions.value; }
    | createMaterializedView { $value = $createMaterializedView.value; }
    | dropTable       { $value = $dropTable.value; }
    ;

query returns [Query value]
    : ^(QUERY selectStmt) { $value = $selectStmt.value; }
    ;

selectStmt returns [Query value]
    : selectClause
      fromClause?
      whereClause?
      (groupClause havingClause?)?
      orderClause?
      limitClause?
        { $value = new Query(
            $selectClause.value,
            $fromClause.value,
            Optional.fromNullable($whereClause.value),
            Objects.firstNonNull($groupClause.value, ImmutableList.<Expression>of()),
            Optional.fromNullable($havingClause.value),
            Objects.firstNonNull($orderClause.value, ImmutableList.<SortItem>of()),
            Optional.fromNullable($limitClause.value));
        }
    ;

restrictedSelectStmt returns [Query value]
    : selectClause fromClause
        { $value = new Query(
            $selectClause.value,
            $fromClause.value,
            Optional.<Expression>absent(),
            ImmutableList.<Expression>of(),
            Optional.<Expression>absent(),
            ImmutableList.<SortItem>of(),
            Optional.<String>absent());
        }
    ;

selectClause returns [Select value]
    : ^(SELECT d=distinct ALL_COLUMNS)  { $value = new Select($d.value, ImmutableList.<Expression>of(new AllColumns())); }
    | ^(SELECT d=distinct s=selectList) { $value = new Select($d.value, $s.value); }
    ;

distinct returns [boolean value]
    : DISTINCT { $value = true; }
    |          { $value = false; }
    ;

selectList returns [List<Expression> value = new ArrayList<>()]
    : ^(SELECT_LIST ( selectItem { $value.add($selectItem.value); } )+ )
    ;

selectItem returns [Expression value]
    : (^(SELECT_ITEM expr ident)) => ^(SELECT_ITEM e=expr i=ident)  { $value = new AliasedExpression($e.value, $i.value); }
    | ^(SELECT_ITEM expr)            { $value = $expr.value; }
    | ^(ALL_COLUMNS q=qname)         { $value = new AllColumns($q.value); }
    ;

fromClause returns [List<Relation> value]
    : ^(FROM t=relationList) { $value = $t.value; }
    ;

whereClause returns [Expression value]
    : ^(WHERE expr) { $value = $expr.value; }
    ;

groupClause returns [List<Expression> value]
    : ^(GROUP_BY exprList) { $value = $exprList.value; }
    ;

havingClause returns [Expression value]
    : ^(HAVING expr) { $value = $expr.value; }
    ;

orderClause returns [List<SortItem> value = new ArrayList<>()]
    : ^(ORDER_BY ( sortItem { $value.add($sortItem.value); } )+ )
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

relationList returns [List<Relation> value = new ArrayList<>()]
    : ( relation { $value.add($relation.value); } )+
    ;

relation returns [Relation value]
    : namedTable       { $value = $namedTable.value; }
    | subqueryRelation { $value = $subqueryRelation.value; }
    | joinedTable      { $value = $joinedTable.value; }
    | joinRelation     { $value = $joinRelation.value; }
    ;

namedTable returns [Relation value]
    : ^(TABLE qname { $value = new Table($qname.value); }
        a=aliasedRelation[$value]? { if ($a.value != null) $value = $a.value; } )
    ;

subqueryRelation returns [Relation value]
    : ^(SUBQUERY subquery { $value = new Subquery($subquery.value); }
        a=aliasedRelation[$value]? { if ($a.value != null) $value = $a.value; } )
    ;

joinedTable returns [Relation value]
    : ^(JOINED_TABLE relation { $value = $relation.value; }
        a=aliasedRelation[$value]? { if ($a.value != null) $value = $a.value; } )
    ;

aliasedRelation[Relation r] returns [AliasedRelation value]
    : ^(TABLE_ALIAS i=ident c=aliasedColumns?) { $value = new AliasedRelation($r, $i.value, $c.value); }
    ;

aliasedColumns returns [List<String> value]
    : ^(ALIASED_COLUMNS identList) { $value = $identList.value; }
    ;

joinRelation returns [Join value]
    : ^(CROSS_JOIN a=relation b=relation)                               { $value = new Join(Join.Type.CROSS, $a.value, $b.value, null); }
    | ^(QUALIFIED_JOIN t=joinType c=joinCriteria a=relation b=relation) { $value = new Join($t.value, $a.value, $b.value, $c.value); }
    ;

joinType returns [Join.Type value]
    : INNER_JOIN { $value = Join.Type.INNER; }
    | LEFT_JOIN  { $value = Join.Type.LEFT; }
    | RIGHT_JOIN { $value = Join.Type.RIGHT; }
    | FULL_JOIN  { $value = Join.Type.FULL; }
    ;

joinCriteria returns [JoinCriteria value]
    : NATURAL            { $value = new NaturalJoin(); }
    | ^(ON expr)         { $value = new JoinOn($expr.value); }
    | ^(USING identList) { $value = new JoinUsing($identList.value); }
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
    | TRUE                  { $value = BooleanLiteral.TRUE_LITERAL; }
    | FALSE                 { $value = BooleanLiteral.FALSE_LITERAL; }
    | intervalValue         { $value = $intervalValue.value; }
    | predicate             { $value = $predicate.value; }
    | ^(IN_LIST exprList)   { $value = new InListExpression($exprList.value); }
    | ^(NEGATIVE e=expr)    { $value = new NegativeExpression($e.value); }
    | caseExpression        { $value = $caseExpression.value; }
    | subquery              { $value = new SubqueryExpression($subquery.value); }
    | extract               { $value = $extract.value; }
    | current_time          { $value = $current_time.value; }
    | cast                  { $value = $cast.value; }
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
    : i=IDENT        { $value = $i.text; }
    | q=QUOTED_IDENT { $value = $q.text; }
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
    : ^(FUNCTION_CALL n=qname w=window? d=distinct a=exprList) { $value = new FunctionCall($n.value, $w.value, $d.value, $a.value); }
    ;

window returns [Window value]
    : ^(WINDOW windowPartition? orderClause? windowFrame?)
        { $value = new Window(
            Objects.firstNonNull($windowPartition.value, ImmutableList.<Expression>of()),
            Objects.firstNonNull($orderClause.value, ImmutableList.<SortItem>of()),
            $windowFrame.value);
        }
    ;

windowPartition returns [List<Expression> value = new ArrayList<>()]
    : ^(PARTITION_BY exprList) { $value = $exprList.value; }
    ;

windowFrame returns [WindowFrame value]
    : ^(RANGE s=frameBound e=frameBound?) { $value = new WindowFrame(WindowFrame.Type.RANGE, $s.value, $e.value); }
    | ^(ROWS s=frameBound e=frameBound?)  { $value = new WindowFrame(WindowFrame.Type.ROWS, $s.value, $e.value); }
    ;

frameBound returns [FrameBound value]
    : UNBOUNDED_PRECEDING { $value = new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING); }
    | UNBOUNDED_FOLLOWING { $value = new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING); }
    | CURRENT_ROW         { $value = new FrameBound(FrameBound.Type.CURRENT_ROW); }
    | ^(PRECEDING expr)   { $value = new FrameBound(FrameBound.Type.PRECEDING, $expr.value); }
    | ^(FOLLOWING expr)   { $value = new FrameBound(FrameBound.Type.FOLLOWING, $expr.value); }
    ;

extract returns [Extract value]
    : ^(EXTRACT field=IDENT expr) { $value = new Extract($expr.value, Extract.Field.valueOf($field.text.toUpperCase())); }
    ;

cast returns [Cast value]
    : ^(CAST expr IDENT) { $value = new Cast($expr.value, $IDENT.text); }
    ;

current_time returns [CurrentTime value]
    : CURRENT_DATE                   { $value = new CurrentTime(CurrentTime.Type.DATE); }
    | CURRENT_TIME                   { $value = new CurrentTime(CurrentTime.Type.TIME); }
    | CURRENT_TIMESTAMP              { $value = new CurrentTime(CurrentTime.Type.TIMESTAMP); }
    | ^(CURRENT_TIME integer)        { $value = new CurrentTime(CurrentTime.Type.TIME, Integer.valueOf($integer.value)); }
    | ^(CURRENT_TIMESTAMP integer)   { $value = new CurrentTime(CurrentTime.Type.TIMESTAMP, Integer.valueOf($integer.value)); }
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

showTables returns [Statement value]
    : ^(SHOW_TABLES from=showTablesFrom? like=showTablesLike?) { $value = new ShowTables($from.value, $like.value); }
    ;

showTablesFrom returns [QualifiedName value]
    : ^(FROM qname) { $value = $qname.value; }
    ;

showTablesLike returns [String value]
    : ^(LIKE string) { $value = $string.value; }
    ;

showColumns returns [Statement value]
    : ^(SHOW_COLUMNS qname) { $value = new ShowColumns($qname.value); }
    ;

showPartitions returns [Statement value]
    : ^(SHOW_PARTITIONS qname) { $value = new ShowPartitions($qname.value); }
    ;

showFunctions returns [Statement value]
    : SHOW_FUNCTIONS { $value = new ShowFunctions(); }
    ;

createMaterializedView returns [Statement value]
    : ^(CREATE_MATERIALIZED_VIEW qname restrictedSelectStmt) { $value = new CreateMaterializedView($qname.value, $restrictedSelectStmt.value); }
    ;

dropTable returns [Statement value]
    : ^(DROP_TABLE qname) { $value = new DropTable($qname.value); }
    ;
