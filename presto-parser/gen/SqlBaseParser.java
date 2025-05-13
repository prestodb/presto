// Generated from /Users/mima0000/ideaproj/presto/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		ADD=10, ADMIN=11, ALL=12, ALTER=13, ANALYZE=14, AND=15, ANY=16, ARRAY=17, 
		AS=18, ASC=19, AT=20, BERNOULLI=21, BETWEEN=22, BY=23, CALL=24, CALLED=25, 
		CASCADE=26, CASE=27, CAST=28, CATALOGS=29, COLUMN=30, COLUMNS=31, COMMENT=32, 
		COMMIT=33, COMMITTED=34, CONSTRAINT=35, CREATE=36, CROSS=37, CUBE=38, 
		CURRENT=39, CURRENT_DATE=40, CURRENT_ROLE=41, CURRENT_TIME=42, CURRENT_TIMESTAMP=43, 
		CURRENT_USER=44, DATA=45, DATE=46, DAY=47, DEALLOCATE=48, DEFINER=49, 
		DELETE=50, DESC=51, DESCRIBE=52, DETERMINISTIC=53, DISTINCT=54, DISTRIBUTED=55, 
		DROP=56, ELSE=57, END=58, ESCAPE=59, EXCEPT=60, EXCLUDING=61, EXECUTE=62, 
		EXISTS=63, EXPLAIN=64, EXTRACT=65, EXTERNAL=66, FALSE=67, FETCH=68, FILTER=69, 
		FIRST=70, FOLLOWING=71, FOR=72, FORMAT=73, FROM=74, FULL=75, FUNCTION=76, 
		FUNCTIONS=77, GRANT=78, GRANTED=79, GRANTS=80, GRAPHVIZ=81, GROUP=82, 
		GROUPING=83, GROUPS=84, HAVING=85, HOUR=86, IF=87, IGNORE=88, IN=89, INCLUDING=90, 
		INNER=91, INPUT=92, INSERT=93, INTERSECT=94, INTERVAL=95, INTO=96, INVOKER=97, 
		IO=98, IS=99, ISOLATION=100, JSON=101, JOIN=102, LANGUAGE=103, LAST=104, 
		LATERAL=105, LEFT=106, LEVEL=107, LIKE=108, LIMIT=109, LOCALTIME=110, 
		LOCALTIMESTAMP=111, LOGICAL=112, MAP=113, MATERIALIZED=114, MINUTE=115, 
		MONTH=116, NAME=117, NATURAL=118, NFC=119, NFD=120, NFKC=121, NFKD=122, 
		NO=123, NONE=124, NORMALIZE=125, NOT=126, NULL=127, NULLIF=128, NULLS=129, 
		OF=130, OFFSET=131, ON=132, ONLY=133, OPTION=134, OR=135, ORDER=136, ORDINALITY=137, 
		OUTER=138, OUTPUT=139, OVER=140, PARTITION=141, PARTITIONS=142, POSITION=143, 
		PRECEDING=144, PREPARE=145, PRIVILEGES=146, PROPERTIES=147, RANGE=148, 
		READ=149, RECURSIVE=150, REFRESH=151, RENAME=152, REPEATABLE=153, REPLACE=154, 
		RESET=155, RESPECT=156, RESTRICT=157, RETURN=158, RETURNS=159, REVOKE=160, 
		RIGHT=161, ROLE=162, ROLES=163, ROLLBACK=164, ROLLUP=165, ROW=166, ROWS=167, 
		SCHEMA=168, SCHEMAS=169, SECOND=170, SECURITY=171, SELECT=172, SERIALIZABLE=173, 
		SESSION=174, SET=175, SETS=176, SHOW=177, SOME=178, SQL=179, START=180, 
		STATS=181, SUBSTRING=182, SYSTEM=183, SYSTEM_TIME=184, SYSTEM_VERSION=185, 
		TABLE=186, TABLES=187, TABLESAMPLE=188, TEMPORARY=189, TEXT=190, THEN=191, 
		TIME=192, TIMESTAMP=193, TO=194, TRANSACTION=195, TRUE=196, TRUNCATE=197, 
		TRY_CAST=198, TYPE=199, UESCAPE=200, UNBOUNDED=201, UNCOMMITTED=202, UNION=203, 
		UNNEST=204, UPDATE=205, USE=206, USER=207, USING=208, VALIDATE=209, VALUES=210, 
		VERBOSE=211, VERSION=212, VIEW=213, WHEN=214, WHERE=215, WITH=216, WORK=217, 
		WRITE=218, YEAR=219, ZONE=220, EQ=221, NEQ=222, LT=223, LTE=224, GT=225, 
		GTE=226, PLUS=227, MINUS=228, ASTERISK=229, SLASH=230, PERCENT=231, CONCAT=232, 
		STRING=233, UNICODE_STRING=234, BINARY_LITERAL=235, INTEGER_VALUE=236, 
		DECIMAL_VALUE=237, DOUBLE_VALUE=238, IDENTIFIER=239, DIGIT_IDENTIFIER=240, 
		QUOTED_IDENTIFIER=241, BACKQUOTED_IDENTIFIER=242, TIME_WITH_TIME_ZONE=243, 
		TIMESTAMP_WITH_TIME_ZONE=244, DOUBLE_PRECISION=245, SIMPLE_COMMENT=246, 
		BRACKETED_COMMENT=247, WS=248, UNRECOGNIZED=249, DELIMITER=250;
	public static final int
		RULE_singleStatement = 0, RULE_standaloneExpression = 1, RULE_standaloneRoutineBody = 2, 
		RULE_statement = 3, RULE_query = 4, RULE_with = 5, RULE_tableElement = 6, 
		RULE_columnDefinition = 7, RULE_likeClause = 8, RULE_properties = 9, RULE_property = 10, 
		RULE_sqlParameterDeclaration = 11, RULE_routineCharacteristics = 12, RULE_routineCharacteristic = 13, 
		RULE_alterRoutineCharacteristics = 14, RULE_alterRoutineCharacteristic = 15, 
		RULE_routineBody = 16, RULE_returnStatement = 17, RULE_externalBodyReference = 18, 
		RULE_language = 19, RULE_determinism = 20, RULE_nullCallClause = 21, RULE_externalRoutineName = 22, 
		RULE_queryNoWith = 23, RULE_queryTerm = 24, RULE_queryPrimary = 25, RULE_sortItem = 26, 
		RULE_querySpecification = 27, RULE_groupBy = 28, RULE_groupingElement = 29, 
		RULE_groupingSet = 30, RULE_namedQuery = 31, RULE_setQuantifier = 32, 
		RULE_selectItem = 33, RULE_relation = 34, RULE_joinType = 35, RULE_joinCriteria = 36, 
		RULE_sampledRelation = 37, RULE_sampleType = 38, RULE_aliasedRelation = 39, 
		RULE_columnAliases = 40, RULE_relationPrimary = 41, RULE_expression = 42, 
		RULE_booleanExpression = 43, RULE_predicate = 44, RULE_valueExpression = 45, 
		RULE_primaryExpression = 46, RULE_string = 47, RULE_nullTreatment = 48, 
		RULE_timeZoneSpecifier = 49, RULE_comparisonOperator = 50, RULE_comparisonQuantifier = 51, 
		RULE_booleanValue = 52, RULE_interval = 53, RULE_intervalField = 54, RULE_normalForm = 55, 
		RULE_types = 56, RULE_type = 57, RULE_typeParameter = 58, RULE_baseType = 59, 
		RULE_whenClause = 60, RULE_filter = 61, RULE_over = 62, RULE_windowFrame = 63, 
		RULE_frameBound = 64, RULE_updateAssignment = 65, RULE_explainOption = 66, 
		RULE_transactionMode = 67, RULE_levelOfIsolation = 68, RULE_callArgument = 69, 
		RULE_privilege = 70, RULE_qualifiedName = 71, RULE_tableVersionExpression = 72, 
		RULE_grantor = 73, RULE_principal = 74, RULE_roles = 75, RULE_identifier = 76, 
		RULE_number = 77, RULE_nonReserved = 78;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "standaloneExpression", "standaloneRoutineBody", "statement", 
			"query", "with", "tableElement", "columnDefinition", "likeClause", "properties", 
			"property", "sqlParameterDeclaration", "routineCharacteristics", "routineCharacteristic", 
			"alterRoutineCharacteristics", "alterRoutineCharacteristic", "routineBody", 
			"returnStatement", "externalBodyReference", "language", "determinism", 
			"nullCallClause", "externalRoutineName", "queryNoWith", "queryTerm", 
			"queryPrimary", "sortItem", "querySpecification", "groupBy", "groupingElement", 
			"groupingSet", "namedQuery", "setQuantifier", "selectItem", "relation", 
			"joinType", "joinCriteria", "sampledRelation", "sampleType", "aliasedRelation", 
			"columnAliases", "relationPrimary", "expression", "booleanExpression", 
			"predicate", "valueExpression", "primaryExpression", "string", "nullTreatment", 
			"timeZoneSpecifier", "comparisonOperator", "comparisonQuantifier", "booleanValue", 
			"interval", "intervalField", "normalForm", "types", "type", "typeParameter", 
			"baseType", "whenClause", "filter", "over", "windowFrame", "frameBound", 
			"updateAssignment", "explainOption", "transactionMode", "levelOfIsolation", 
			"callArgument", "privilege", "qualifiedName", "tableVersionExpression", 
			"grantor", "principal", "roles", "identifier", "number", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'.'", "'('", "')'", "','", "'?'", "'->'", "'['", "']'", "'=>'", 
			"'ADD'", "'ADMIN'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", "'ANY'", 
			"'ARRAY'", "'AS'", "'ASC'", "'AT'", "'BERNOULLI'", "'BETWEEN'", "'BY'", 
			"'CALL'", "'CALLED'", "'CASCADE'", "'CASE'", "'CAST'", "'CATALOGS'", 
			"'COLUMN'", "'COLUMNS'", "'COMMENT'", "'COMMIT'", "'COMMITTED'", "'CONSTRAINT'", 
			"'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'", "'CURRENT_DATE'", "'CURRENT_ROLE'", 
			"'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", "'CURRENT_USER'", "'DATA'", 
			"'DATE'", "'DAY'", "'DEALLOCATE'", "'DEFINER'", "'DELETE'", "'DESC'", 
			"'DESCRIBE'", "'DETERMINISTIC'", "'DISTINCT'", "'DISTRIBUTED'", "'DROP'", 
			"'ELSE'", "'END'", "'ESCAPE'", "'EXCEPT'", "'EXCLUDING'", "'EXECUTE'", 
			"'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'EXTERNAL'", "'FALSE'", "'FETCH'", 
			"'FILTER'", "'FIRST'", "'FOLLOWING'", "'FOR'", "'FORMAT'", "'FROM'", 
			"'FULL'", "'FUNCTION'", "'FUNCTIONS'", "'GRANT'", "'GRANTED'", "'GRANTS'", 
			"'GRAPHVIZ'", "'GROUP'", "'GROUPING'", "'GROUPS'", "'HAVING'", "'HOUR'", 
			"'IF'", "'IGNORE'", "'IN'", "'INCLUDING'", "'INNER'", "'INPUT'", "'INSERT'", 
			"'INTERSECT'", "'INTERVAL'", "'INTO'", "'INVOKER'", "'IO'", "'IS'", "'ISOLATION'", 
			"'JSON'", "'JOIN'", "'LANGUAGE'", "'LAST'", "'LATERAL'", "'LEFT'", "'LEVEL'", 
			"'LIKE'", "'LIMIT'", "'LOCALTIME'", "'LOCALTIMESTAMP'", "'LOGICAL'", 
			"'MAP'", "'MATERIALIZED'", "'MINUTE'", "'MONTH'", "'NAME'", "'NATURAL'", 
			"'NFC'", "'NFD'", "'NFKC'", "'NFKD'", "'NO'", "'NONE'", "'NORMALIZE'", 
			"'NOT'", "'NULL'", "'NULLIF'", "'NULLS'", "'OF'", "'OFFSET'", "'ON'", 
			"'ONLY'", "'OPTION'", "'OR'", "'ORDER'", "'ORDINALITY'", "'OUTER'", "'OUTPUT'", 
			"'OVER'", "'PARTITION'", "'PARTITIONS'", "'POSITION'", "'PRECEDING'", 
			"'PREPARE'", "'PRIVILEGES'", "'PROPERTIES'", "'RANGE'", "'READ'", "'RECURSIVE'", 
			"'REFRESH'", "'RENAME'", "'REPEATABLE'", "'REPLACE'", "'RESET'", "'RESPECT'", 
			"'RESTRICT'", "'RETURN'", "'RETURNS'", "'REVOKE'", "'RIGHT'", "'ROLE'", 
			"'ROLES'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", "'ROWS'", "'SCHEMA'", "'SCHEMAS'", 
			"'SECOND'", "'SECURITY'", "'SELECT'", "'SERIALIZABLE'", "'SESSION'", 
			"'SET'", "'SETS'", "'SHOW'", "'SOME'", "'SQL'", "'START'", "'STATS'", 
			"'SUBSTRING'", "'SYSTEM'", "'SYSTEM_TIME'", "'SYSTEM_VERSION'", "'TABLE'", 
			"'TABLES'", "'TABLESAMPLE'", "'TEMPORARY'", "'TEXT'", "'THEN'", "'TIME'", 
			"'TIMESTAMP'", "'TO'", "'TRANSACTION'", "'TRUE'", "'TRUNCATE'", "'TRY_CAST'", 
			"'TYPE'", "'UESCAPE'", "'UNBOUNDED'", "'UNCOMMITTED'", "'UNION'", "'UNNEST'", 
			"'UPDATE'", "'USE'", "'USER'", "'USING'", "'VALIDATE'", "'VALUES'", "'VERBOSE'", 
			"'VERSION'", "'VIEW'", "'WHEN'", "'WHERE'", "'WITH'", "'WORK'", "'WRITE'", 
			"'YEAR'", "'ZONE'", "'='", null, "'<'", "'<='", "'>'", "'>='", "'+'", 
			"'-'", "'*'", "'/'", "'%'", "'||'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, "ADD", "ADMIN", 
			"ALL", "ALTER", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "AT", 
			"BERNOULLI", "BETWEEN", "BY", "CALL", "CALLED", "CASCADE", "CASE", "CAST", 
			"CATALOGS", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMMITTED", "CONSTRAINT", 
			"CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_ROLE", 
			"CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATA", "DATE", 
			"DAY", "DEALLOCATE", "DEFINER", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", 
			"DISTINCT", "DISTRIBUTED", "DROP", "ELSE", "END", "ESCAPE", "EXCEPT", 
			"EXCLUDING", "EXECUTE", "EXISTS", "EXPLAIN", "EXTRACT", "EXTERNAL", "FALSE", 
			"FETCH", "FILTER", "FIRST", "FOLLOWING", "FOR", "FORMAT", "FROM", "FULL", 
			"FUNCTION", "FUNCTIONS", "GRANT", "GRANTED", "GRANTS", "GRAPHVIZ", "GROUP", 
			"GROUPING", "GROUPS", "HAVING", "HOUR", "IF", "IGNORE", "IN", "INCLUDING", 
			"INNER", "INPUT", "INSERT", "INTERSECT", "INTERVAL", "INTO", "INVOKER", 
			"IO", "IS", "ISOLATION", "JSON", "JOIN", "LANGUAGE", "LAST", "LATERAL", 
			"LEFT", "LEVEL", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "LOGICAL", 
			"MAP", "MATERIALIZED", "MINUTE", "MONTH", "NAME", "NATURAL", "NFC", "NFD", 
			"NFKC", "NFKD", "NO", "NONE", "NORMALIZE", "NOT", "NULL", "NULLIF", "NULLS", 
			"OF", "OFFSET", "ON", "ONLY", "OPTION", "OR", "ORDER", "ORDINALITY", 
			"OUTER", "OUTPUT", "OVER", "PARTITION", "PARTITIONS", "POSITION", "PRECEDING", 
			"PREPARE", "PRIVILEGES", "PROPERTIES", "RANGE", "READ", "RECURSIVE", 
			"REFRESH", "RENAME", "REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTRICT", 
			"RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLES", "ROLLBACK", 
			"ROLLUP", "ROW", "ROWS", "SCHEMA", "SCHEMAS", "SECOND", "SECURITY", "SELECT", 
			"SERIALIZABLE", "SESSION", "SET", "SETS", "SHOW", "SOME", "SQL", "START", 
			"STATS", "SUBSTRING", "SYSTEM", "SYSTEM_TIME", "SYSTEM_VERSION", "TABLE", 
			"TABLES", "TABLESAMPLE", "TEMPORARY", "TEXT", "THEN", "TIME", "TIMESTAMP", 
			"TO", "TRANSACTION", "TRUE", "TRUNCATE", "TRY_CAST", "TYPE", "UESCAPE", 
			"UNBOUNDED", "UNCOMMITTED", "UNION", "UNNEST", "UPDATE", "USE", "USER", 
			"USING", "VALIDATE", "VALUES", "VERBOSE", "VERSION", "VIEW", "WHEN", 
			"WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE", "EQ", "NEQ", "LT", 
			"LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
			"CONCAT", "STRING", "UNICODE_STRING", "BINARY_LITERAL", "INTEGER_VALUE", 
			"DECIMAL_VALUE", "DOUBLE_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER", 
			"BACKQUOTED_IDENTIFIER", "TIME_WITH_TIME_ZONE", "TIMESTAMP_WITH_TIME_ZONE", 
			"DOUBLE_PRECISION", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED", 
			"DELIMITER"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(158);
			statement();
			setState(159);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StandaloneExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneExpressionContext standaloneExpression() throws RecognitionException {
		StandaloneExpressionContext _localctx = new StandaloneExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_standaloneExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(161);
			expression();
			setState(162);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StandaloneRoutineBodyContext extends ParserRuleContext {
		public RoutineBodyContext routineBody() {
			return getRuleContext(RoutineBodyContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneRoutineBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneRoutineBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneRoutineBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneRoutineBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneRoutineBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneRoutineBodyContext standaloneRoutineBody() throws RecognitionException {
		StandaloneRoutineBodyContext _localctx = new StandaloneRoutineBodyContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_standaloneRoutineBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(164);
			routineBody();
			setState(165);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public List<ExplainOptionContext> explainOption() {
			return getRuleContexts(ExplainOptionContext.class);
		}
		public ExplainOptionContext explainOption(int i) {
			return getRuleContext(ExplainOptionContext.class,i);
		}
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PrepareContext extends StatementContext {
		public TerminalNode PREPARE() { return getToken(SqlBaseParser.PREPARE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public PrepareContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrepare(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrepare(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrepare(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropMaterializedViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UseContext extends StatementContext {
		public IdentifierContext schema;
		public IdentifierContext catalog;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeallocateContext extends StatementContext {
		public TerminalNode DEALLOCATE() { return getToken(SqlBaseParser.DEALLOCATE, 0); }
		public TerminalNode PREPARE() { return getToken(SqlBaseParser.PREPARE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DeallocateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeallocate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeallocate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeallocate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommitContext extends StatementContext {
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public CommitContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommit(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateRoleContext extends StatementContext {
		public IdentifierContext name;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public CreateRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateFunctionContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TypesContext types() {
			return getRuleContext(TypesContext.class,0);
		}
		public ShowCreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public QualifiedNameContext column;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public DropColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTablesContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCatalogsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowCatalogsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCatalogs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCatalogs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCatalogs(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowRolesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public ShowRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext from;
		public IdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public RenameColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RevokeRolesContext extends StatementContext {
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public RolesContext roles() {
			return getRuleContext(RolesContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<PrincipalContext> principal() {
			return getRuleContexts(PrincipalContext.class);
		}
		public PrincipalContext principal(int i) {
			return getRuleContext(PrincipalContext.class,i);
		}
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public RevokeRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRevokeRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRevokeRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRevokeRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowColumnsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowRoleGrantsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public ShowRoleGrantsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowRoleGrants(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowRoleGrants(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowRoleGrants(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public ColumnDefinitionContext column;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public AddColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetSessionContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ResetSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertIntoContext extends StatementContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public InsertIntoContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertInto(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertInto(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertInto(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowSessionContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateSchemaContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExecuteContext extends StatementContext {
		public TerminalNode EXECUTE() { return getToken(SqlBaseParser.EXECUTE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ExecuteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExecute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExecute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExecute(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameSchemaContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RenameSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropRoleContext extends StatementContext {
		public IdentifierContext name;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetRoleContext extends StatementContext {
		public IdentifierContext role;
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode NONE() { return getToken(SqlBaseParser.NONE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public SetRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateFunctionContext extends StatementContext {
		public QualifiedNameContext functionName;
		public TypeContext returnType;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode RETURNS() { return getToken(SqlBaseParser.RETURNS, 0); }
		public RoutineCharacteristicsContext routineCharacteristics() {
			return getRuleContext(RoutineCharacteristicsContext.class,0);
		}
		public RoutineBodyContext routineBody() {
			return getRuleContext(RoutineBodyContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public List<SqlParameterDeclarationContext> sqlParameterDeclaration() {
			return getRuleContexts(SqlParameterDeclarationContext.class);
		}
		public SqlParameterDeclarationContext sqlParameterDeclaration(int i) {
			return getRuleContext(SqlParameterDeclarationContext.class,i);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowGrantsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public ShowGrantsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowGrants(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowGrants(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowGrants(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropSchemaContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public DropSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateViewContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<TableElementContext> tableElement() {
			return getRuleContexts(TableElementContext.class);
		}
		public TableElementContext tableElement(int i) {
			return getRuleContext(TableElementContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StartTransactionContext extends StatementContext {
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public List<TransactionModeContext> transactionMode() {
			return getRuleContexts(TransactionModeContext.class);
		}
		public TransactionModeContext transactionMode(int i) {
			return getRuleContext(TransactionModeContext.class,i);
		}
		public StartTransactionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStartTransaction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStartTransaction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStartTransaction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableAsSelectContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public CreateTableAsSelectContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableAsSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableAsSelect(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableAsSelect(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowStatsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowStats(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowStats(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowStats(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TypesContext types() {
			return getRuleContext(TypesContext.class,0);
		}
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RevokeContext extends StatementContext {
		public PrincipalContext grantee;
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public List<PrivilegeContext> privilege() {
			return getRuleContexts(PrivilegeContext.class);
		}
		public PrivilegeContext privilege(int i) {
			return getRuleContext(PrivilegeContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public RevokeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRevoke(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRevoke(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRevoke(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UpdateContext extends StatementContext {
		public BooleanExpressionContext where;
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public List<UpdateAssignmentContext> updateAssignment() {
			return getRuleContexts(UpdateAssignmentContext.class);
		}
		public UpdateAssignmentContext updateAssignment(int i) {
			return getRuleContext(UpdateAssignmentContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public UpdateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUpdate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTypeContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<SqlParameterDeclarationContext> sqlParameterDeclaration() {
			return getRuleContexts(SqlParameterDeclarationContext.class);
		}
		public SqlParameterDeclarationContext sqlParameterDeclaration(int i) {
			return getRuleContext(SqlParameterDeclarationContext.class,i);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public CreateTypeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeleteContext extends StatementContext {
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public DeleteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDelete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDelete(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeInputContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DescribeInputContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeInput(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeInput(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeInput(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatsForQueryContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public ShowStatsForQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowStatsForQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowStatsForQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowStatsForQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateMaterializedViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AlterFunctionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public AlterRoutineCharacteristicsContext alterRoutineCharacteristics() {
			return getRuleContext(AlterRoutineCharacteristicsContext.class,0);
		}
		public TypesContext types() {
			return getRuleContext(TypesContext.class,0);
		}
		public AlterFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAlterFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetSessionContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowSchemasContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowSchemasContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSchemas(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSchemas(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSchemas(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollbackContext extends StatementContext {
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public RollbackContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollback(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollback(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollback(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GrantRolesContext extends StatementContext {
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public RolesContext roles() {
			return getRuleContext(RolesContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PrincipalContext> principal() {
			return getRuleContexts(PrincipalContext.class);
		}
		public PrincipalContext principal(int i) {
			return getRuleContext(PrincipalContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public GrantRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrantRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrantRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrantRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CallContext extends StatementContext {
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<CallArgumentContext> callArgument() {
			return getRuleContexts(CallArgumentContext.class);
		}
		public CallArgumentContext callArgument(int i) {
			return getRuleContext(CallArgumentContext.class,i);
		}
		public CallContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshMaterializedViewContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public RefreshMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateMaterializedViewContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowFunctionsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeOutputContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DescribeOutputContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeOutput(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeOutput(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeOutput(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GrantContext extends StatementContext {
		public PrincipalContext grantee;
		public List<TerminalNode> GRANT() { return getTokens(SqlBaseParser.GRANT); }
		public TerminalNode GRANT(int i) {
			return getToken(SqlBaseParser.GRANT, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public List<PrivilegeContext> privilege() {
			return getRuleContexts(PrivilegeContext.class);
		}
		public PrivilegeContext privilege(int i) {
			return getRuleContext(PrivilegeContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public GrantContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrant(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_statement);
		int _la;
		try {
			setState(825);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(167);
				query();
				}
				break;
			case 2:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(168);
				match(USE);
				setState(169);
				((UseContext)_localctx).schema = identifier();
				}
				break;
			case 3:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(170);
				match(USE);
				setState(171);
				((UseContext)_localctx).catalog = identifier();
				setState(172);
				match(T__0);
				setState(173);
				((UseContext)_localctx).schema = identifier();
				}
				break;
			case 4:
				_localctx = new CreateSchemaContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(175);
				match(CREATE);
				setState(176);
				match(SCHEMA);
				setState(180);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(177);
					match(IF);
					setState(178);
					match(NOT);
					setState(179);
					match(EXISTS);
					}
					break;
				}
				setState(182);
				qualifiedName();
				setState(185);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(183);
					match(WITH);
					setState(184);
					properties();
					}
				}

				}
				break;
			case 5:
				_localctx = new DropSchemaContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(187);
				match(DROP);
				setState(188);
				match(SCHEMA);
				setState(191);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
				case 1:
					{
					setState(189);
					match(IF);
					setState(190);
					match(EXISTS);
					}
					break;
				}
				setState(193);
				qualifiedName();
				setState(195);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(194);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 6:
				_localctx = new RenameSchemaContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(197);
				match(ALTER);
				setState(198);
				match(SCHEMA);
				setState(199);
				qualifiedName();
				setState(200);
				match(RENAME);
				setState(201);
				match(TO);
				setState(202);
				identifier();
				}
				break;
			case 7:
				_localctx = new CreateTableAsSelectContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(204);
				match(CREATE);
				setState(205);
				match(TABLE);
				setState(209);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(206);
					match(IF);
					setState(207);
					match(NOT);
					setState(208);
					match(EXISTS);
					}
					break;
				}
				setState(211);
				qualifiedName();
				setState(213);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(212);
					columnAliases();
					}
				}

				setState(217);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(215);
					match(COMMENT);
					setState(216);
					string();
					}
				}

				setState(221);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(219);
					match(WITH);
					setState(220);
					properties();
					}
				}

				setState(223);
				match(AS);
				setState(229);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
				case 1:
					{
					setState(224);
					query();
					}
					break;
				case 2:
					{
					setState(225);
					match(T__1);
					setState(226);
					query();
					setState(227);
					match(T__2);
					}
					break;
				}
				setState(236);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(231);
					match(WITH);
					setState(233);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==NO) {
						{
						setState(232);
						match(NO);
						}
					}

					setState(235);
					match(DATA);
					}
				}

				}
				break;
			case 8:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(238);
				match(CREATE);
				setState(239);
				match(TABLE);
				setState(243);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(240);
					match(IF);
					setState(241);
					match(NOT);
					setState(242);
					match(EXISTS);
					}
					break;
				}
				setState(245);
				qualifiedName();
				setState(246);
				match(T__1);
				setState(247);
				tableElement();
				setState(252);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(248);
					match(T__3);
					setState(249);
					tableElement();
					}
					}
					setState(254);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(255);
				match(T__2);
				setState(258);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(256);
					match(COMMENT);
					setState(257);
					string();
					}
				}

				setState(262);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(260);
					match(WITH);
					setState(261);
					properties();
					}
				}

				}
				break;
			case 9:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(264);
				match(DROP);
				setState(265);
				match(TABLE);
				setState(268);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(266);
					match(IF);
					setState(267);
					match(EXISTS);
					}
					break;
				}
				setState(270);
				qualifiedName();
				}
				break;
			case 10:
				_localctx = new InsertIntoContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(271);
				match(INSERT);
				setState(272);
				match(INTO);
				setState(273);
				qualifiedName();
				setState(275);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
				case 1:
					{
					setState(274);
					columnAliases();
					}
					break;
				}
				setState(277);
				query();
				}
				break;
			case 11:
				_localctx = new DeleteContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(279);
				match(DELETE);
				setState(280);
				match(FROM);
				setState(281);
				qualifiedName();
				setState(284);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(282);
					match(WHERE);
					setState(283);
					booleanExpression(0);
					}
				}

				}
				break;
			case 12:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(286);
				match(TRUNCATE);
				setState(287);
				match(TABLE);
				setState(288);
				qualifiedName();
				}
				break;
			case 13:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(289);
				match(ALTER);
				setState(290);
				match(TABLE);
				setState(293);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
				case 1:
					{
					setState(291);
					match(IF);
					setState(292);
					match(EXISTS);
					}
					break;
				}
				setState(295);
				((RenameTableContext)_localctx).from = qualifiedName();
				setState(296);
				match(RENAME);
				setState(297);
				match(TO);
				setState(298);
				((RenameTableContext)_localctx).to = qualifiedName();
				}
				break;
			case 14:
				_localctx = new RenameColumnContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(300);
				match(ALTER);
				setState(301);
				match(TABLE);
				setState(304);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
				case 1:
					{
					setState(302);
					match(IF);
					setState(303);
					match(EXISTS);
					}
					break;
				}
				setState(306);
				((RenameColumnContext)_localctx).tableName = qualifiedName();
				setState(307);
				match(RENAME);
				setState(308);
				match(COLUMN);
				setState(311);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
				case 1:
					{
					setState(309);
					match(IF);
					setState(310);
					match(EXISTS);
					}
					break;
				}
				setState(313);
				((RenameColumnContext)_localctx).from = identifier();
				setState(314);
				match(TO);
				setState(315);
				((RenameColumnContext)_localctx).to = identifier();
				}
				break;
			case 15:
				_localctx = new DropColumnContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(317);
				match(ALTER);
				setState(318);
				match(TABLE);
				setState(321);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
				case 1:
					{
					setState(319);
					match(IF);
					setState(320);
					match(EXISTS);
					}
					break;
				}
				setState(323);
				((DropColumnContext)_localctx).tableName = qualifiedName();
				setState(324);
				match(DROP);
				setState(325);
				match(COLUMN);
				setState(328);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
				case 1:
					{
					setState(326);
					match(IF);
					setState(327);
					match(EXISTS);
					}
					break;
				}
				setState(330);
				((DropColumnContext)_localctx).column = qualifiedName();
				}
				break;
			case 16:
				_localctx = new AddColumnContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(332);
				match(ALTER);
				setState(333);
				match(TABLE);
				setState(336);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
				case 1:
					{
					setState(334);
					match(IF);
					setState(335);
					match(EXISTS);
					}
					break;
				}
				setState(338);
				((AddColumnContext)_localctx).tableName = qualifiedName();
				setState(339);
				match(ADD);
				setState(340);
				match(COLUMN);
				setState(344);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
				case 1:
					{
					setState(341);
					match(IF);
					setState(342);
					match(NOT);
					setState(343);
					match(EXISTS);
					}
					break;
				}
				setState(346);
				((AddColumnContext)_localctx).column = columnDefinition();
				}
				break;
			case 17:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(348);
				match(ANALYZE);
				setState(349);
				qualifiedName();
				setState(352);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(350);
					match(WITH);
					setState(351);
					properties();
					}
				}

				}
				break;
			case 18:
				_localctx = new CreateTypeContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(354);
				match(CREATE);
				setState(355);
				match(TYPE);
				setState(356);
				qualifiedName();
				setState(357);
				match(AS);
				setState(370);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case T__1:
					{
					setState(358);
					match(T__1);
					setState(359);
					sqlParameterDeclaration();
					setState(364);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(360);
						match(T__3);
						setState(361);
						sqlParameterDeclaration();
						}
						}
						setState(366);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(367);
					match(T__2);
					}
					break;
				case ADD:
				case ADMIN:
				case ALL:
				case ANALYZE:
				case ANY:
				case ARRAY:
				case ASC:
				case AT:
				case BERNOULLI:
				case CALL:
				case CALLED:
				case CASCADE:
				case CATALOGS:
				case COLUMN:
				case COLUMNS:
				case COMMENT:
				case COMMIT:
				case COMMITTED:
				case CURRENT:
				case CURRENT_ROLE:
				case DATA:
				case DATE:
				case DAY:
				case DEFINER:
				case DESC:
				case DETERMINISTIC:
				case DISTRIBUTED:
				case EXCLUDING:
				case EXPLAIN:
				case EXTERNAL:
				case FETCH:
				case FILTER:
				case FIRST:
				case FOLLOWING:
				case FORMAT:
				case FUNCTION:
				case FUNCTIONS:
				case GRANT:
				case GRANTED:
				case GRANTS:
				case GRAPHVIZ:
				case GROUPS:
				case HOUR:
				case IF:
				case IGNORE:
				case INCLUDING:
				case INPUT:
				case INTERVAL:
				case INVOKER:
				case IO:
				case ISOLATION:
				case JSON:
				case LANGUAGE:
				case LAST:
				case LATERAL:
				case LEVEL:
				case LIMIT:
				case LOGICAL:
				case MAP:
				case MATERIALIZED:
				case MINUTE:
				case MONTH:
				case NAME:
				case NFC:
				case NFD:
				case NFKC:
				case NFKD:
				case NO:
				case NONE:
				case NULLIF:
				case NULLS:
				case OF:
				case OFFSET:
				case ONLY:
				case OPTION:
				case ORDINALITY:
				case OUTPUT:
				case OVER:
				case PARTITION:
				case PARTITIONS:
				case POSITION:
				case PRECEDING:
				case PRIVILEGES:
				case PROPERTIES:
				case RANGE:
				case READ:
				case REFRESH:
				case RENAME:
				case REPEATABLE:
				case REPLACE:
				case RESET:
				case RESPECT:
				case RESTRICT:
				case RETURN:
				case RETURNS:
				case REVOKE:
				case ROLE:
				case ROLES:
				case ROLLBACK:
				case ROW:
				case ROWS:
				case SCHEMA:
				case SCHEMAS:
				case SECOND:
				case SECURITY:
				case SERIALIZABLE:
				case SESSION:
				case SET:
				case SETS:
				case SHOW:
				case SOME:
				case SQL:
				case START:
				case STATS:
				case SUBSTRING:
				case SYSTEM:
				case SYSTEM_TIME:
				case SYSTEM_VERSION:
				case TABLES:
				case TABLESAMPLE:
				case TEMPORARY:
				case TEXT:
				case TIME:
				case TIMESTAMP:
				case TO:
				case TRANSACTION:
				case TRUNCATE:
				case TRY_CAST:
				case TYPE:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UPDATE:
				case USE:
				case USER:
				case VALIDATE:
				case VERBOSE:
				case VERSION:
				case VIEW:
				case WORK:
				case WRITE:
				case YEAR:
				case ZONE:
				case IDENTIFIER:
				case DIGIT_IDENTIFIER:
				case QUOTED_IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
				case TIME_WITH_TIME_ZONE:
				case TIMESTAMP_WITH_TIME_ZONE:
				case DOUBLE_PRECISION:
					{
					setState(369);
					type(0);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 19:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(372);
				match(CREATE);
				setState(375);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(373);
					match(OR);
					setState(374);
					match(REPLACE);
					}
				}

				setState(377);
				match(VIEW);
				setState(378);
				qualifiedName();
				setState(381);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SECURITY) {
					{
					setState(379);
					match(SECURITY);
					setState(380);
					_la = _input.LA(1);
					if ( !(_la==DEFINER || _la==INVOKER) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(383);
				match(AS);
				setState(384);
				query();
				}
				break;
			case 20:
				_localctx = new DropViewContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(386);
				match(DROP);
				setState(387);
				match(VIEW);
				setState(390);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
				case 1:
					{
					setState(388);
					match(IF);
					setState(389);
					match(EXISTS);
					}
					break;
				}
				setState(392);
				qualifiedName();
				}
				break;
			case 21:
				_localctx = new CreateMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(393);
				match(CREATE);
				setState(394);
				match(MATERIALIZED);
				setState(395);
				match(VIEW);
				setState(399);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(396);
					match(IF);
					setState(397);
					match(NOT);
					setState(398);
					match(EXISTS);
					}
					break;
				}
				setState(401);
				qualifiedName();
				setState(404);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(402);
					match(COMMENT);
					setState(403);
					string();
					}
				}

				setState(408);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(406);
					match(WITH);
					setState(407);
					properties();
					}
				}

				setState(410);
				match(AS);
				setState(416);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
				case 1:
					{
					setState(411);
					query();
					}
					break;
				case 2:
					{
					setState(412);
					match(T__1);
					setState(413);
					query();
					setState(414);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 22:
				_localctx = new DropMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(418);
				match(DROP);
				setState(419);
				match(MATERIALIZED);
				setState(420);
				match(VIEW);
				setState(423);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
				case 1:
					{
					setState(421);
					match(IF);
					setState(422);
					match(EXISTS);
					}
					break;
				}
				setState(425);
				qualifiedName();
				}
				break;
			case 23:
				_localctx = new RefreshMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(426);
				match(REFRESH);
				setState(427);
				match(MATERIALIZED);
				setState(428);
				match(VIEW);
				setState(429);
				qualifiedName();
				setState(430);
				match(WHERE);
				setState(431);
				booleanExpression(0);
				}
				break;
			case 24:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(433);
				match(CREATE);
				setState(436);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(434);
					match(OR);
					setState(435);
					match(REPLACE);
					}
				}

				setState(439);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(438);
					match(TEMPORARY);
					}
				}

				setState(441);
				match(FUNCTION);
				setState(442);
				((CreateFunctionContext)_localctx).functionName = qualifiedName();
				setState(443);
				match(T__1);
				setState(452);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 2353942828582394880L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2287595198925239029L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 2111062832506607L) != 0)) {
					{
					setState(444);
					sqlParameterDeclaration();
					setState(449);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(445);
						match(T__3);
						setState(446);
						sqlParameterDeclaration();
						}
						}
						setState(451);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(454);
				match(T__2);
				setState(455);
				match(RETURNS);
				setState(456);
				((CreateFunctionContext)_localctx).returnType = type(0);
				setState(459);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(457);
					match(COMMENT);
					setState(458);
					string();
					}
				}

				setState(461);
				routineCharacteristics();
				setState(462);
				routineBody();
				}
				break;
			case 25:
				_localctx = new AlterFunctionContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(464);
				match(ALTER);
				setState(465);
				match(FUNCTION);
				setState(466);
				qualifiedName();
				setState(468);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(467);
					types();
					}
				}

				setState(470);
				alterRoutineCharacteristics();
				}
				break;
			case 26:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(472);
				match(DROP);
				setState(474);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(473);
					match(TEMPORARY);
					}
				}

				setState(476);
				match(FUNCTION);
				setState(479);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
				case 1:
					{
					setState(477);
					match(IF);
					setState(478);
					match(EXISTS);
					}
					break;
				}
				setState(481);
				qualifiedName();
				setState(483);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(482);
					types();
					}
				}

				}
				break;
			case 27:
				_localctx = new CallContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(485);
				match(CALL);
				setState(486);
				qualifiedName();
				setState(487);
				match(T__1);
				setState(496);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -6869397322032522204L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -18036704055397633L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 11256903631562495L) != 0)) {
					{
					setState(488);
					callArgument();
					setState(493);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(489);
						match(T__3);
						setState(490);
						callArgument();
						}
						}
						setState(495);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(498);
				match(T__2);
				}
				break;
			case 28:
				_localctx = new CreateRoleContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(500);
				match(CREATE);
				setState(501);
				match(ROLE);
				setState(502);
				((CreateRoleContext)_localctx).name = identifier();
				setState(506);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(503);
					match(WITH);
					setState(504);
					match(ADMIN);
					setState(505);
					grantor();
					}
				}

				}
				break;
			case 29:
				_localctx = new DropRoleContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(508);
				match(DROP);
				setState(509);
				match(ROLE);
				setState(510);
				((DropRoleContext)_localctx).name = identifier();
				}
				break;
			case 30:
				_localctx = new GrantRolesContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(511);
				match(GRANT);
				setState(512);
				roles();
				setState(513);
				match(TO);
				setState(514);
				principal();
				setState(519);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(515);
					match(T__3);
					setState(516);
					principal();
					}
					}
					setState(521);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(525);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(522);
					match(WITH);
					setState(523);
					match(ADMIN);
					setState(524);
					match(OPTION);
					}
				}

				setState(530);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANTED) {
					{
					setState(527);
					match(GRANTED);
					setState(528);
					match(BY);
					setState(529);
					grantor();
					}
				}

				}
				break;
			case 31:
				_localctx = new RevokeRolesContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(532);
				match(REVOKE);
				setState(536);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
				case 1:
					{
					setState(533);
					match(ADMIN);
					setState(534);
					match(OPTION);
					setState(535);
					match(FOR);
					}
					break;
				}
				setState(538);
				roles();
				setState(539);
				match(FROM);
				setState(540);
				principal();
				setState(545);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(541);
					match(T__3);
					setState(542);
					principal();
					}
					}
					setState(547);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(551);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANTED) {
					{
					setState(548);
					match(GRANTED);
					setState(549);
					match(BY);
					setState(550);
					grantor();
					}
				}

				}
				break;
			case 32:
				_localctx = new SetRoleContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(553);
				match(SET);
				setState(554);
				match(ROLE);
				setState(558);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
				case 1:
					{
					setState(555);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(556);
					match(NONE);
					}
					break;
				case 3:
					{
					setState(557);
					((SetRoleContext)_localctx).role = identifier();
					}
					break;
				}
				}
				break;
			case 33:
				_localctx = new GrantContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(560);
				match(GRANT);
				setState(571);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
				case 1:
					{
					setState(561);
					privilege();
					setState(566);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(562);
						match(T__3);
						setState(563);
						privilege();
						}
						}
						setState(568);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case 2:
					{
					setState(569);
					match(ALL);
					setState(570);
					match(PRIVILEGES);
					}
					break;
				}
				setState(573);
				match(ON);
				setState(575);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TABLE) {
					{
					setState(574);
					match(TABLE);
					}
				}

				setState(577);
				qualifiedName();
				setState(578);
				match(TO);
				setState(579);
				((GrantContext)_localctx).grantee = principal();
				setState(583);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(580);
					match(WITH);
					setState(581);
					match(GRANT);
					setState(582);
					match(OPTION);
					}
				}

				}
				break;
			case 34:
				_localctx = new RevokeContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(585);
				match(REVOKE);
				setState(589);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
				case 1:
					{
					setState(586);
					match(GRANT);
					setState(587);
					match(OPTION);
					setState(588);
					match(FOR);
					}
					break;
				}
				setState(601);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
				case 1:
					{
					setState(591);
					privilege();
					setState(596);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(592);
						match(T__3);
						setState(593);
						privilege();
						}
						}
						setState(598);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case 2:
					{
					setState(599);
					match(ALL);
					setState(600);
					match(PRIVILEGES);
					}
					break;
				}
				setState(603);
				match(ON);
				setState(605);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TABLE) {
					{
					setState(604);
					match(TABLE);
					}
				}

				setState(607);
				qualifiedName();
				setState(608);
				match(FROM);
				setState(609);
				((RevokeContext)_localctx).grantee = principal();
				}
				break;
			case 35:
				_localctx = new ShowGrantsContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(611);
				match(SHOW);
				setState(612);
				match(GRANTS);
				setState(618);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(613);
					match(ON);
					setState(615);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==TABLE) {
						{
						setState(614);
						match(TABLE);
						}
					}

					setState(617);
					qualifiedName();
					}
				}

				}
				break;
			case 36:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(620);
				match(EXPLAIN);
				setState(622);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
				case 1:
					{
					setState(621);
					match(ANALYZE);
					}
					break;
				}
				setState(625);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==VERBOSE) {
					{
					setState(624);
					match(VERBOSE);
					}
				}

				setState(638);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
				case 1:
					{
					setState(627);
					match(T__1);
					setState(628);
					explainOption();
					setState(633);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(629);
						match(T__3);
						setState(630);
						explainOption();
						}
						}
						setState(635);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(636);
					match(T__2);
					}
					break;
				}
				setState(640);
				statement();
				}
				break;
			case 37:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(641);
				match(SHOW);
				setState(642);
				match(CREATE);
				setState(643);
				match(TABLE);
				setState(644);
				qualifiedName();
				}
				break;
			case 38:
				_localctx = new ShowCreateViewContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(645);
				match(SHOW);
				setState(646);
				match(CREATE);
				setState(647);
				match(VIEW);
				setState(648);
				qualifiedName();
				}
				break;
			case 39:
				_localctx = new ShowCreateMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(649);
				match(SHOW);
				setState(650);
				match(CREATE);
				setState(651);
				match(MATERIALIZED);
				setState(652);
				match(VIEW);
				setState(653);
				qualifiedName();
				}
				break;
			case 40:
				_localctx = new ShowCreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(654);
				match(SHOW);
				setState(655);
				match(CREATE);
				setState(656);
				match(FUNCTION);
				setState(657);
				qualifiedName();
				setState(659);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(658);
					types();
					}
				}

				}
				break;
			case 41:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(661);
				match(SHOW);
				setState(662);
				match(TABLES);
				setState(665);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(663);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(664);
					qualifiedName();
					}
				}

				setState(673);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(667);
					match(LIKE);
					setState(668);
					((ShowTablesContext)_localctx).pattern = string();
					setState(671);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(669);
						match(ESCAPE);
						setState(670);
						((ShowTablesContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 42:
				_localctx = new ShowSchemasContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(675);
				match(SHOW);
				setState(676);
				match(SCHEMAS);
				setState(679);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(677);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(678);
					identifier();
					}
				}

				setState(687);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(681);
					match(LIKE);
					setState(682);
					((ShowSchemasContext)_localctx).pattern = string();
					setState(685);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(683);
						match(ESCAPE);
						setState(684);
						((ShowSchemasContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 43:
				_localctx = new ShowCatalogsContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(689);
				match(SHOW);
				setState(690);
				match(CATALOGS);
				setState(697);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(691);
					match(LIKE);
					setState(692);
					((ShowCatalogsContext)_localctx).pattern = string();
					setState(695);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(693);
						match(ESCAPE);
						setState(694);
						((ShowCatalogsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 44:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(699);
				match(SHOW);
				setState(700);
				match(COLUMNS);
				setState(701);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(702);
				qualifiedName();
				}
				break;
			case 45:
				_localctx = new ShowStatsContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(703);
				match(SHOW);
				setState(704);
				match(STATS);
				setState(705);
				match(FOR);
				setState(706);
				qualifiedName();
				}
				break;
			case 46:
				_localctx = new ShowStatsForQueryContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(707);
				match(SHOW);
				setState(708);
				match(STATS);
				setState(709);
				match(FOR);
				setState(710);
				match(T__1);
				setState(711);
				querySpecification();
				setState(712);
				match(T__2);
				}
				break;
			case 47:
				_localctx = new ShowRolesContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(714);
				match(SHOW);
				setState(716);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CURRENT) {
					{
					setState(715);
					match(CURRENT);
					}
				}

				setState(718);
				match(ROLES);
				setState(721);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(719);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(720);
					identifier();
					}
				}

				}
				break;
			case 48:
				_localctx = new ShowRoleGrantsContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(723);
				match(SHOW);
				setState(724);
				match(ROLE);
				setState(725);
				match(GRANTS);
				setState(728);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(726);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(727);
					identifier();
					}
				}

				}
				break;
			case 49:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(730);
				match(DESCRIBE);
				setState(731);
				qualifiedName();
				}
				break;
			case 50:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(732);
				match(DESC);
				setState(733);
				qualifiedName();
				}
				break;
			case 51:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(734);
				match(SHOW);
				setState(735);
				match(FUNCTIONS);
				setState(742);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(736);
					match(LIKE);
					setState(737);
					((ShowFunctionsContext)_localctx).pattern = string();
					setState(740);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(738);
						match(ESCAPE);
						setState(739);
						((ShowFunctionsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 52:
				_localctx = new ShowSessionContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(744);
				match(SHOW);
				setState(745);
				match(SESSION);
				setState(752);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(746);
					match(LIKE);
					setState(747);
					((ShowSessionContext)_localctx).pattern = string();
					setState(750);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(748);
						match(ESCAPE);
						setState(749);
						((ShowSessionContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 53:
				_localctx = new SetSessionContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(754);
				match(SET);
				setState(755);
				match(SESSION);
				setState(756);
				qualifiedName();
				setState(757);
				match(EQ);
				setState(758);
				expression();
				}
				break;
			case 54:
				_localctx = new ResetSessionContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(760);
				match(RESET);
				setState(761);
				match(SESSION);
				setState(762);
				qualifiedName();
				}
				break;
			case 55:
				_localctx = new StartTransactionContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(763);
				match(START);
				setState(764);
				match(TRANSACTION);
				setState(773);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ISOLATION || _la==READ) {
					{
					setState(765);
					transactionMode();
					setState(770);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(766);
						match(T__3);
						setState(767);
						transactionMode();
						}
						}
						setState(772);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 56:
				_localctx = new CommitContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(775);
				match(COMMIT);
				setState(777);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WORK) {
					{
					setState(776);
					match(WORK);
					}
				}

				}
				break;
			case 57:
				_localctx = new RollbackContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(779);
				match(ROLLBACK);
				setState(781);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WORK) {
					{
					setState(780);
					match(WORK);
					}
				}

				}
				break;
			case 58:
				_localctx = new PrepareContext(_localctx);
				enterOuterAlt(_localctx, 58);
				{
				setState(783);
				match(PREPARE);
				setState(784);
				identifier();
				setState(785);
				match(FROM);
				setState(786);
				statement();
				}
				break;
			case 59:
				_localctx = new DeallocateContext(_localctx);
				enterOuterAlt(_localctx, 59);
				{
				setState(788);
				match(DEALLOCATE);
				setState(789);
				match(PREPARE);
				setState(790);
				identifier();
				}
				break;
			case 60:
				_localctx = new ExecuteContext(_localctx);
				enterOuterAlt(_localctx, 60);
				{
				setState(791);
				match(EXECUTE);
				setState(792);
				identifier();
				setState(802);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(793);
					match(USING);
					setState(794);
					expression();
					setState(799);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(795);
						match(T__3);
						setState(796);
						expression();
						}
						}
						setState(801);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 61:
				_localctx = new DescribeInputContext(_localctx);
				enterOuterAlt(_localctx, 61);
				{
				setState(804);
				match(DESCRIBE);
				setState(805);
				match(INPUT);
				setState(806);
				identifier();
				}
				break;
			case 62:
				_localctx = new DescribeOutputContext(_localctx);
				enterOuterAlt(_localctx, 62);
				{
				setState(807);
				match(DESCRIBE);
				setState(808);
				match(OUTPUT);
				setState(809);
				identifier();
				}
				break;
			case 63:
				_localctx = new UpdateContext(_localctx);
				enterOuterAlt(_localctx, 63);
				{
				setState(810);
				match(UPDATE);
				setState(811);
				qualifiedName();
				setState(812);
				match(SET);
				setState(813);
				updateAssignment();
				setState(818);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(814);
					match(T__3);
					setState(815);
					updateAssignment();
					}
					}
					setState(820);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(823);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(821);
					match(WHERE);
					setState(822);
					((UpdateContext)_localctx).where = booleanExpression(0);
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public WithContext with() {
			return getRuleContext(WithContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(828);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(827);
				with();
				}
			}

			setState(830);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WithContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public TerminalNode RECURSIVE() { return getToken(SqlBaseParser.RECURSIVE, 0); }
		public WithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WithContext with() throws RecognitionException {
		WithContext _localctx = new WithContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_with);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(832);
			match(WITH);
			setState(834);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RECURSIVE) {
				{
				setState(833);
				match(RECURSIVE);
				}
			}

			setState(836);
			namedQuery();
			setState(841);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(837);
				match(T__3);
				setState(838);
				namedQuery();
				}
				}
				setState(843);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableElementContext extends ParserRuleContext {
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public LikeClauseContext likeClause() {
			return getRuleContext(LikeClauseContext.class,0);
		}
		public TableElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableElement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableElement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableElementContext tableElement() throws RecognitionException {
		TableElementContext _localctx = new TableElementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_tableElement);
		try {
			setState(846);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_ROLE:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case EXTERNAL:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(844);
				columnDefinition();
				}
				break;
			case LIKE:
				enterOuterAlt(_localctx, 2);
				{
				setState(845);
				likeClause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColumnDefinitionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public ColumnDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnDefinitionContext columnDefinition() throws RecognitionException {
		ColumnDefinitionContext _localctx = new ColumnDefinitionContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_columnDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(848);
			identifier();
			setState(849);
			type(0);
			setState(852);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(850);
				match(NOT);
				setState(851);
				match(NULL);
				}
			}

			setState(856);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(854);
				match(COMMENT);
				setState(855);
				string();
				}
			}

			setState(860);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(858);
				match(WITH);
				setState(859);
				properties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LikeClauseContext extends ParserRuleContext {
		public Token optionType;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public LikeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_likeClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLikeClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLikeClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLikeClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LikeClauseContext likeClause() throws RecognitionException {
		LikeClauseContext _localctx = new LikeClauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_likeClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(862);
			match(LIKE);
			setState(863);
			qualifiedName();
			setState(866);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXCLUDING || _la==INCLUDING) {
				{
				setState(864);
				((LikeClauseContext)_localctx).optionType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==EXCLUDING || _la==INCLUDING) ) {
					((LikeClauseContext)_localctx).optionType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(865);
				match(PROPERTIES);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertiesContext extends ParserRuleContext {
		public List<PropertyContext> property() {
			return getRuleContexts(PropertyContext.class);
		}
		public PropertyContext property(int i) {
			return getRuleContext(PropertyContext.class,i);
		}
		public PropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertiesContext properties() throws RecognitionException {
		PropertiesContext _localctx = new PropertiesContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_properties);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(868);
			match(T__1);
			setState(869);
			property();
			setState(874);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(870);
				match(T__3);
				setState(871);
				property();
				}
				}
				setState(876);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(877);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyContext property() throws RecognitionException {
		PropertyContext _localctx = new PropertyContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_property);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(879);
			identifier();
			setState(880);
			match(EQ);
			setState(881);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SqlParameterDeclarationContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public SqlParameterDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sqlParameterDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSqlParameterDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSqlParameterDeclaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSqlParameterDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SqlParameterDeclarationContext sqlParameterDeclaration() throws RecognitionException {
		SqlParameterDeclarationContext _localctx = new SqlParameterDeclarationContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_sqlParameterDeclaration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(883);
			identifier();
			setState(884);
			type(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RoutineCharacteristicsContext extends ParserRuleContext {
		public List<RoutineCharacteristicContext> routineCharacteristic() {
			return getRuleContexts(RoutineCharacteristicContext.class);
		}
		public RoutineCharacteristicContext routineCharacteristic(int i) {
			return getRuleContext(RoutineCharacteristicContext.class,i);
		}
		public RoutineCharacteristicsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_routineCharacteristics; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRoutineCharacteristics(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRoutineCharacteristics(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRoutineCharacteristics(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RoutineCharacteristicsContext routineCharacteristics() throws RecognitionException {
		RoutineCharacteristicsContext _localctx = new RoutineCharacteristicsContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_routineCharacteristics);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(889);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CALLED || _la==DETERMINISTIC || ((((_la - 103)) & ~0x3f) == 0 && ((1L << (_la - 103)) & 72057594046316545L) != 0)) {
				{
				{
				setState(886);
				routineCharacteristic();
				}
				}
				setState(891);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RoutineCharacteristicContext extends ParserRuleContext {
		public TerminalNode LANGUAGE() { return getToken(SqlBaseParser.LANGUAGE, 0); }
		public LanguageContext language() {
			return getRuleContext(LanguageContext.class,0);
		}
		public DeterminismContext determinism() {
			return getRuleContext(DeterminismContext.class,0);
		}
		public NullCallClauseContext nullCallClause() {
			return getRuleContext(NullCallClauseContext.class,0);
		}
		public RoutineCharacteristicContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_routineCharacteristic; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRoutineCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRoutineCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRoutineCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RoutineCharacteristicContext routineCharacteristic() throws RecognitionException {
		RoutineCharacteristicContext _localctx = new RoutineCharacteristicContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_routineCharacteristic);
		try {
			setState(896);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LANGUAGE:
				enterOuterAlt(_localctx, 1);
				{
				setState(892);
				match(LANGUAGE);
				setState(893);
				language();
				}
				break;
			case DETERMINISTIC:
			case NOT:
				enterOuterAlt(_localctx, 2);
				{
				setState(894);
				determinism();
				}
				break;
			case CALLED:
			case RETURNS:
				enterOuterAlt(_localctx, 3);
				{
				setState(895);
				nullCallClause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AlterRoutineCharacteristicsContext extends ParserRuleContext {
		public List<AlterRoutineCharacteristicContext> alterRoutineCharacteristic() {
			return getRuleContexts(AlterRoutineCharacteristicContext.class);
		}
		public AlterRoutineCharacteristicContext alterRoutineCharacteristic(int i) {
			return getRuleContext(AlterRoutineCharacteristicContext.class,i);
		}
		public AlterRoutineCharacteristicsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterRoutineCharacteristics; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterRoutineCharacteristics(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterRoutineCharacteristics(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAlterRoutineCharacteristics(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AlterRoutineCharacteristicsContext alterRoutineCharacteristics() throws RecognitionException {
		AlterRoutineCharacteristicsContext _localctx = new AlterRoutineCharacteristicsContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_alterRoutineCharacteristics);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(901);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CALLED || _la==RETURNS) {
				{
				{
				setState(898);
				alterRoutineCharacteristic();
				}
				}
				setState(903);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AlterRoutineCharacteristicContext extends ParserRuleContext {
		public NullCallClauseContext nullCallClause() {
			return getRuleContext(NullCallClauseContext.class,0);
		}
		public AlterRoutineCharacteristicContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterRoutineCharacteristic; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterRoutineCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterRoutineCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAlterRoutineCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AlterRoutineCharacteristicContext alterRoutineCharacteristic() throws RecognitionException {
		AlterRoutineCharacteristicContext _localctx = new AlterRoutineCharacteristicContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_alterRoutineCharacteristic);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(904);
			nullCallClause();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RoutineBodyContext extends ParserRuleContext {
		public ReturnStatementContext returnStatement() {
			return getRuleContext(ReturnStatementContext.class,0);
		}
		public ExternalBodyReferenceContext externalBodyReference() {
			return getRuleContext(ExternalBodyReferenceContext.class,0);
		}
		public RoutineBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_routineBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRoutineBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRoutineBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRoutineBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RoutineBodyContext routineBody() throws RecognitionException {
		RoutineBodyContext _localctx = new RoutineBodyContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_routineBody);
		try {
			setState(908);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case RETURN:
				enterOuterAlt(_localctx, 1);
				{
				setState(906);
				returnStatement();
				}
				break;
			case EXTERNAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(907);
				externalBodyReference();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ReturnStatementContext extends ParserRuleContext {
		public TerminalNode RETURN() { return getToken(SqlBaseParser.RETURN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ReturnStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_returnStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReturnStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReturnStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReturnStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ReturnStatementContext returnStatement() throws RecognitionException {
		ReturnStatementContext _localctx = new ReturnStatementContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_returnStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(910);
			match(RETURN);
			setState(911);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExternalBodyReferenceContext extends ParserRuleContext {
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode NAME() { return getToken(SqlBaseParser.NAME, 0); }
		public ExternalRoutineNameContext externalRoutineName() {
			return getRuleContext(ExternalRoutineNameContext.class,0);
		}
		public ExternalBodyReferenceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_externalBodyReference; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExternalBodyReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExternalBodyReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExternalBodyReference(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExternalBodyReferenceContext externalBodyReference() throws RecognitionException {
		ExternalBodyReferenceContext _localctx = new ExternalBodyReferenceContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_externalBodyReference);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(913);
			match(EXTERNAL);
			setState(916);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NAME) {
				{
				setState(914);
				match(NAME);
				setState(915);
				externalRoutineName();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LanguageContext extends ParserRuleContext {
		public TerminalNode SQL() { return getToken(SqlBaseParser.SQL, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public LanguageContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_language; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLanguage(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLanguage(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLanguage(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LanguageContext language() throws RecognitionException {
		LanguageContext _localctx = new LanguageContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_language);
		try {
			setState(920);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(918);
				match(SQL);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(919);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DeterminismContext extends ParserRuleContext {
		public TerminalNode DETERMINISTIC() { return getToken(SqlBaseParser.DETERMINISTIC, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DeterminismContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_determinism; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeterminism(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeterminism(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeterminism(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeterminismContext determinism() throws RecognitionException {
		DeterminismContext _localctx = new DeterminismContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_determinism);
		try {
			setState(925);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DETERMINISTIC:
				enterOuterAlt(_localctx, 1);
				{
				setState(922);
				match(DETERMINISTIC);
				}
				break;
			case NOT:
				enterOuterAlt(_localctx, 2);
				{
				setState(923);
				match(NOT);
				setState(924);
				match(DETERMINISTIC);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NullCallClauseContext extends ParserRuleContext {
		public TerminalNode RETURNS() { return getToken(SqlBaseParser.RETURNS, 0); }
		public List<TerminalNode> NULL() { return getTokens(SqlBaseParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(SqlBaseParser.NULL, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public TerminalNode CALLED() { return getToken(SqlBaseParser.CALLED, 0); }
		public NullCallClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullCallClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullCallClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullCallClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullCallClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullCallClauseContext nullCallClause() throws RecognitionException {
		NullCallClauseContext _localctx = new NullCallClauseContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_nullCallClause);
		try {
			setState(936);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case RETURNS:
				enterOuterAlt(_localctx, 1);
				{
				setState(927);
				match(RETURNS);
				setState(928);
				match(NULL);
				setState(929);
				match(ON);
				setState(930);
				match(NULL);
				setState(931);
				match(INPUT);
				}
				break;
			case CALLED:
				enterOuterAlt(_localctx, 2);
				{
				setState(932);
				match(CALLED);
				setState(933);
				match(ON);
				setState(934);
				match(NULL);
				setState(935);
				match(INPUT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExternalRoutineNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExternalRoutineNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_externalRoutineName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExternalRoutineName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExternalRoutineName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExternalRoutineName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExternalRoutineNameContext externalRoutineName() throws RecognitionException {
		ExternalRoutineNameContext _localctx = new ExternalRoutineNameContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_externalRoutineName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(938);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryNoWithContext extends ParserRuleContext {
		public Token offset;
		public Token limit;
		public Token fetchFirstNRows;
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode OFFSET() { return getToken(SqlBaseParser.OFFSET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public List<TerminalNode> ROWS() { return getTokens(SqlBaseParser.ROWS); }
		public TerminalNode ROWS(int i) {
			return getToken(SqlBaseParser.ROWS, i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryNoWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryNoWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryNoWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_queryNoWith);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(940);
			queryTerm(0);
			setState(951);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(941);
				match(ORDER);
				setState(942);
				match(BY);
				setState(943);
				sortItem();
				setState(948);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(944);
					match(T__3);
					setState(945);
					sortItem();
					}
					}
					setState(950);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(958);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(953);
				match(OFFSET);
				setState(954);
				((QueryNoWithContext)_localctx).offset = match(INTEGER_VALUE);
				setState(956);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW || _la==ROWS) {
					{
					setState(955);
					_la = _input.LA(1);
					if ( !(_la==ROW || _la==ROWS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
			}

			setState(969);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
			case 1:
				{
				setState(967);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LIMIT:
					{
					setState(960);
					match(LIMIT);
					setState(961);
					((QueryNoWithContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((QueryNoWithContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				case FETCH:
					{
					{
					setState(962);
					match(FETCH);
					setState(963);
					match(FIRST);
					setState(964);
					((QueryNoWithContext)_localctx).fetchFirstNRows = match(INTEGER_VALUE);
					setState(965);
					match(ROWS);
					setState(966);
					match(ONLY);
					}
					}
					break;
				case EOF:
				case T__2:
				case WITH:
					break;
				default:
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 48;
		enterRecursionRule(_localctx, 48, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(972);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(988);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,120,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(986);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,119,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(974);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(975);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(977);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(976);
							setQuantifier();
							}
						}

						setState(979);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(980);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(981);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(983);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(982);
							setQuantifier();
							}
						}

						setState(985);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(990);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,120,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends QueryPrimaryContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public InlineTableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_queryPrimary);
		try {
			int _alt;
			setState(1007);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(991);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(992);
				match(TABLE);
				setState(993);
				qualifiedName();
				}
				break;
			case VALUES:
				_localctx = new InlineTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(994);
				match(VALUES);
				setState(995);
				expression();
				setState(1000);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,121,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(996);
						match(T__3);
						setState(997);
						expression();
						}
						} 
					}
					setState(1002);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,121,_ctx);
				}
				}
				break;
			case T__1:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1003);
				match(T__1);
				setState(1004);
				queryNoWith();
				setState(1005);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrdering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1009);
			expression();
			setState(1011);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1010);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1015);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1013);
				match(NULLS);
				setState(1014);
				((SortItemContext)_localctx).nullOrdering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuerySpecificationContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_querySpecification);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1017);
			match(SELECT);
			setState(1019);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				{
				setState(1018);
				setQuantifier();
				}
				break;
			}
			setState(1021);
			selectItem();
			setState(1026);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,126,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1022);
					match(T__3);
					setState(1023);
					selectItem();
					}
					} 
				}
				setState(1028);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,126,_ctx);
			}
			setState(1038);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,128,_ctx) ) {
			case 1:
				{
				setState(1029);
				match(FROM);
				setState(1030);
				relation(0);
				setState(1035);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,127,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1031);
						match(T__3);
						setState(1032);
						relation(0);
						}
						} 
					}
					setState(1037);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,127,_ctx);
				}
				}
				break;
			}
			setState(1042);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,129,_ctx) ) {
			case 1:
				{
				setState(1040);
				match(WHERE);
				setState(1041);
				((QuerySpecificationContext)_localctx).where = booleanExpression(0);
				}
				break;
			}
			setState(1047);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,130,_ctx) ) {
			case 1:
				{
				setState(1044);
				match(GROUP);
				setState(1045);
				match(BY);
				setState(1046);
				groupBy();
				}
				break;
			}
			setState(1051);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,131,_ctx) ) {
			case 1:
				{
				setState(1049);
				match(HAVING);
				setState(1050);
				((QuerySpecificationContext)_localctx).having = booleanExpression(0);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupByContext extends ParserRuleContext {
		public List<GroupingElementContext> groupingElement() {
			return getRuleContexts(GroupingElementContext.class);
		}
		public GroupingElementContext groupingElement(int i) {
			return getRuleContext(GroupingElementContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public GroupByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupBy(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_groupBy);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1054);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
			case 1:
				{
				setState(1053);
				setQuantifier();
				}
				break;
			}
			setState(1056);
			groupingElement();
			setState(1061);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,133,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1057);
					match(T__3);
					setState(1058);
					groupingElement();
					}
					} 
				}
				setState(1063);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,133,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingElementContext extends ParserRuleContext {
		public GroupingElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingElement; }
	 
		public GroupingElementContext() { }
		public void copyFrom(GroupingElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MultipleGroupingSetsContext extends GroupingElementContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public MultipleGroupingSetsContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipleGroupingSets(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipleGroupingSets(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultipleGroupingSets(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SingleGroupingSetContext extends GroupingElementContext {
		public GroupingSetContext groupingSet() {
			return getRuleContext(GroupingSetContext.class,0);
		}
		public SingleGroupingSetContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CubeContext extends GroupingElementContext {
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public CubeContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCube(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCube(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCube(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollupContext extends GroupingElementContext {
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public RollupContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollup(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollup(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingElementContext groupingElement() throws RecognitionException {
		GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_groupingElement);
		int _la;
		try {
			setState(1104);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				_localctx = new SingleGroupingSetContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1064);
				groupingSet();
				}
				break;
			case 2:
				_localctx = new RollupContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1065);
				match(ROLLUP);
				setState(1066);
				match(T__1);
				setState(1075);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -6869397322032522204L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -18036704055397633L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 11256903631562495L) != 0)) {
					{
					setState(1067);
					expression();
					setState(1072);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1068);
						match(T__3);
						setState(1069);
						expression();
						}
						}
						setState(1074);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1077);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new CubeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1078);
				match(CUBE);
				setState(1079);
				match(T__1);
				setState(1088);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -6869397322032522204L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -18036704055397633L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 11256903631562495L) != 0)) {
					{
					setState(1080);
					expression();
					setState(1085);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1081);
						match(T__3);
						setState(1082);
						expression();
						}
						}
						setState(1087);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1090);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new MultipleGroupingSetsContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1091);
				match(GROUPING);
				setState(1092);
				match(SETS);
				setState(1093);
				match(T__1);
				setState(1094);
				groupingSet();
				setState(1099);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1095);
					match(T__3);
					setState(1096);
					groupingSet();
					}
					}
					setState(1101);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1102);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_groupingSet);
		int _la;
		try {
			setState(1119);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1106);
				match(T__1);
				setState(1115);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -6869397322032522204L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -18036704055397633L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 11256903631562495L) != 0)) {
					{
					setState(1107);
					expression();
					setState(1112);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1108);
						match(T__3);
						setState(1109);
						expression();
						}
						}
						setState(1114);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1117);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1118);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1121);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1123);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(1122);
				columnAliases();
				}
			}

			setState(1125);
			match(AS);
			setState(1126);
			match(T__1);
			setState(1127);
			query();
			setState(1128);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1130);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectItemContext extends ParserRuleContext {
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
	 
		public SelectItemContext() { }
		public void copyFrom(SelectItemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectAllContext extends SelectItemContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public SelectAllContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectAll(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectSingleContext extends SelectItemContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public SelectSingleContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectSingle(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectSingle(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectSingle(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_selectItem);
		int _la;
		try {
			setState(1144);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,146,_ctx) ) {
			case 1:
				_localctx = new SelectSingleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1132);
				expression();
				setState(1137);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
				case 1:
					{
					setState(1134);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1133);
						match(AS);
						}
					}

					setState(1136);
					identifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1139);
				qualifiedName();
				setState(1140);
				match(T__0);
				setState(1141);
				match(ASTERISK);
				}
				break;
			case 3:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1143);
				match(ASTERISK);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationContext extends ParserRuleContext {
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	 
		public RelationContext() { }
		public void copyFrom(RelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RelationDefaultContext extends RelationContext {
		public SampledRelationContext sampledRelation() {
			return getRuleContext(SampledRelationContext.class,0);
		}
		public RelationDefaultContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelationDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelationDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelationDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends RelationContext {
		public RelationContext left;
		public SampledRelationContext right;
		public RelationContext rightRelation;
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public SampledRelationContext sampledRelation() {
			return getRuleContext(SampledRelationContext.class,0);
		}
		public JoinRelationContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		return relation(0);
	}

	private RelationContext relation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RelationContext _localctx = new RelationContext(_ctx, _parentState);
		RelationContext _prevctx = _localctx;
		int _startState = 68;
		enterRecursionRule(_localctx, 68, RULE_relation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new RelationDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1147);
			sampledRelation();
			}
			_ctx.stop = _input.LT(-1);
			setState(1167);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,148,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
					((JoinRelationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_relation);
					setState(1149);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(1163);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case CROSS:
						{
						setState(1150);
						match(CROSS);
						setState(1151);
						match(JOIN);
						setState(1152);
						((JoinRelationContext)_localctx).right = sampledRelation();
						}
						break;
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case RIGHT:
						{
						setState(1153);
						joinType();
						setState(1154);
						match(JOIN);
						setState(1155);
						((JoinRelationContext)_localctx).rightRelation = relation(0);
						setState(1156);
						joinCriteria();
						}
						break;
					case NATURAL:
						{
						setState(1158);
						match(NATURAL);
						setState(1159);
						joinType();
						setState(1160);
						match(JOIN);
						setState(1161);
						((JoinRelationContext)_localctx).right = sampledRelation();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(1169);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,148,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_joinType);
		int _la;
		try {
			setState(1185);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case JOIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1171);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1170);
					match(INNER);
					}
				}

				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1173);
				match(LEFT);
				setState(1175);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1174);
					match(OUTER);
					}
				}

				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1177);
				match(RIGHT);
				setState(1179);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1178);
					match(OUTER);
					}
				}

				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(1181);
				match(FULL);
				setState(1183);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1182);
					match(OUTER);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_joinCriteria);
		int _la;
		try {
			setState(1201);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1187);
				match(ON);
				setState(1188);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1189);
				match(USING);
				setState(1190);
				match(T__1);
				setState(1191);
				identifier();
				setState(1196);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1192);
					match(T__3);
					setState(1193);
					identifier();
					}
					}
					setState(1198);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1199);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampledRelationContext extends ParserRuleContext {
		public ExpressionContext percentage;
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public SampleTypeContext sampleType() {
			return getRuleContext(SampleTypeContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampledRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampledRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampledRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampledRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampledRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampledRelationContext sampledRelation() throws RecognitionException {
		SampledRelationContext _localctx = new SampledRelationContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_sampledRelation);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1203);
			aliasedRelation();
			setState(1210);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,156,_ctx) ) {
			case 1:
				{
				setState(1204);
				match(TABLESAMPLE);
				setState(1205);
				sampleType();
				setState(1206);
				match(T__1);
				setState(1207);
				((SampledRelationContext)_localctx).percentage = expression();
				setState(1208);
				match(T__2);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampleTypeContext extends ParserRuleContext {
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public SampleTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleTypeContext sampleType() throws RecognitionException {
		SampleTypeContext _localctx = new SampleTypeContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_sampleType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1212);
			_la = _input.LA(1);
			if ( !(_la==BERNOULLI || _la==SYSTEM) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AliasedRelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasedRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AliasedRelationContext aliasedRelation() throws RecognitionException {
		AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_aliasedRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1214);
			relationPrimary();
			setState(1222);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,159,_ctx) ) {
			case 1:
				{
				setState(1216);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1215);
					match(AS);
					}
				}

				setState(1218);
				identifier();
				setState(1220);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,158,_ctx) ) {
				case 1:
					{
					setState(1219);
					columnAliases();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColumnAliasesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnAliases; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnAliases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnAliases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnAliases(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnAliasesContext columnAliases() throws RecognitionException {
		ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_columnAliases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1224);
			match(T__1);
			setState(1225);
			identifier();
			setState(1230);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1226);
				match(T__3);
				setState(1227);
				identifier();
				}
				}
				setState(1232);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1233);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryRelationContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public ParenthesizedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnnestContext extends RelationPrimaryContext {
		public TerminalNode UNNEST() { return getToken(SqlBaseParser.UNNEST, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public UnnestContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnnest(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnnest(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnnest(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LateralContext extends RelationPrimaryContext {
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public LateralContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLateral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableVersionExpressionContext tableVersionExpression() {
			return getRuleContext(TableVersionExpressionContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_relationPrimary);
		int _la;
		try {
			setState(1267);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1235);
				qualifiedName();
				setState(1237);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,161,_ctx) ) {
				case 1:
					{
					setState(1236);
					tableVersionExpression();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SubqueryRelationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1239);
				match(T__1);
				setState(1240);
				query();
				setState(1241);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new UnnestContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1243);
				match(UNNEST);
				setState(1244);
				match(T__1);
				setState(1245);
				expression();
				setState(1250);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1246);
					match(T__3);
					setState(1247);
					expression();
					}
					}
					setState(1252);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1253);
				match(T__2);
				setState(1256);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
				case 1:
					{
					setState(1254);
					match(WITH);
					setState(1255);
					match(ORDINALITY);
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new LateralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1258);
				match(LATERAL);
				setState(1259);
				match(T__1);
				setState(1260);
				query();
				setState(1261);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new ParenthesizedRelationContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1263);
				match(T__1);
				setState(1264);
				relation(0);
				setState(1265);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1269);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 86;
		enterRecursionRule(_localctx, 86, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1278);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case T__4:
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CASE:
			case CAST:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_DATE:
			case CURRENT_ROLE:
			case CURRENT_TIME:
			case CURRENT_TIMESTAMP:
			case CURRENT_USER:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case EXTRACT:
			case EXTERNAL:
			case FALSE:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPING:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOCALTIME:
			case LOCALTIMESTAMP:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NORMALIZE:
			case NULL:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUE:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case PLUS:
			case MINUS:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
			case DOUBLE_PRECISION:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1272);
				((PredicatedContext)_localctx).valueExpression = valueExpression(0);
				setState(1274);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
				case 1:
					{
					setState(1273);
					predicate(((PredicatedContext)_localctx).valueExpression);
					}
					break;
				}
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1276);
				match(NOT);
				setState(1277);
				booleanExpression(3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(1288);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,168,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1286);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,167,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1280);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1281);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(1282);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1283);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1284);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(1285);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(1290);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,168,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateContext extends ParserRuleContext {
		public ParserRuleContext value;
		public PredicateContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
		public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
			super(parent, invokingState);
			this.value = value;
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
			this.value = ctx.value;
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends PredicateContext {
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LikeContext extends PredicateContext {
		public ValueExpressionContext pattern;
		public ValueExpressionContext escape;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public LikeContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLike(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InSubqueryContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InSubqueryContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DistinctFromContext extends PredicateContext {
		public ValueExpressionContext right;
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DistinctFromContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDistinctFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDistinctFrom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDistinctFrom(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InListContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InListContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInList(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullPredicateContext extends PredicateContext {
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BetweenContext extends PredicateContext {
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BetweenContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBetween(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBetween(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuantifiedComparisonContext extends PredicateContext {
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ComparisonQuantifierContext comparisonQuantifier() {
			return getRuleContext(ComparisonQuantifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QuantifiedComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuantifiedComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuantifiedComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuantifiedComparison(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
		enterRule(_localctx, 88, RULE_predicate);
		int _la;
		try {
			setState(1352);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
			case 1:
				_localctx = new ComparisonContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1291);
				comparisonOperator();
				setState(1292);
				((ComparisonContext)_localctx).right = valueExpression(0);
				}
				break;
			case 2:
				_localctx = new QuantifiedComparisonContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1294);
				comparisonOperator();
				setState(1295);
				comparisonQuantifier();
				setState(1296);
				match(T__1);
				setState(1297);
				query();
				setState(1298);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new BetweenContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1301);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1300);
					match(NOT);
					}
				}

				setState(1303);
				match(BETWEEN);
				setState(1304);
				((BetweenContext)_localctx).lower = valueExpression(0);
				setState(1305);
				match(AND);
				setState(1306);
				((BetweenContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 4:
				_localctx = new InListContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1309);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1308);
					match(NOT);
					}
				}

				setState(1311);
				match(IN);
				setState(1312);
				match(T__1);
				setState(1313);
				expression();
				setState(1318);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1314);
					match(T__3);
					setState(1315);
					expression();
					}
					}
					setState(1320);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1321);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new InSubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1324);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1323);
					match(NOT);
					}
				}

				setState(1326);
				match(IN);
				setState(1327);
				match(T__1);
				setState(1328);
				query();
				setState(1329);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new LikeContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1332);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1331);
					match(NOT);
					}
				}

				setState(1334);
				match(LIKE);
				setState(1335);
				((LikeContext)_localctx).pattern = valueExpression(0);
				setState(1338);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
				case 1:
					{
					setState(1336);
					match(ESCAPE);
					setState(1337);
					((LikeContext)_localctx).escape = valueExpression(0);
					}
					break;
				}
				}
				break;
			case 7:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1340);
				match(IS);
				setState(1342);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1341);
					match(NOT);
					}
				}

				setState(1344);
				match(NULL);
				}
				break;
			case 8:
				_localctx = new DistinctFromContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(1345);
				match(IS);
				setState(1347);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1346);
					match(NOT);
					}
				}

				setState(1349);
				match(DISTINCT);
				setState(1350);
				match(FROM);
				setState(1351);
				((DistinctFromContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConcatenationContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public TerminalNode CONCAT() { return getToken(SqlBaseParser.CONCAT, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ConcatenationContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AtTimeZoneContext extends ValueExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TimeZoneSpecifierContext timeZoneSpecifier() {
			return getRuleContext(TimeZoneSpecifierContext.class,0);
		}
		public AtTimeZoneContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAtTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAtTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAtTimeZone(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 90;
		enterRecursionRule(_localctx, 90, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1358);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case T__4:
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CASE:
			case CAST:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_DATE:
			case CURRENT_ROLE:
			case CURRENT_TIME:
			case CURRENT_TIMESTAMP:
			case CURRENT_USER:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case EXTRACT:
			case EXTERNAL:
			case FALSE:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPING:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOCALTIME:
			case LOCALTIMESTAMP:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NORMALIZE:
			case NULL:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUE:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
			case DOUBLE_PRECISION:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1355);
				primaryExpression(0);
				}
				break;
			case PLUS:
			case MINUS:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1356);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1357);
				valueExpression(4);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(1374);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,180,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1372);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1360);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1361);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 229)) & ~0x3f) == 0 && ((1L << (_la - 229)) & 7L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1362);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1363);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1364);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1365);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(new ValueExpressionContext(_parentctx, _parentState));
						((ConcatenationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1366);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1367);
						match(CONCAT);
						setState(1368);
						((ConcatenationContext)_localctx).right = valueExpression(2);
						}
						break;
					case 4:
						{
						_localctx = new AtTimeZoneContext(new ValueExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1369);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(1370);
						match(AT);
						setState(1371);
						timeZoneSpecifier();
						}
						break;
					}
					} 
				}
				setState(1376);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,180,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeConstructorContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode DOUBLE_PRECISION() { return getToken(SqlBaseParser.DOUBLE_PRECISION, 0); }
		public TypeConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecialDateTimeFunctionContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode LOCALTIME() { return getToken(SqlBaseParser.LOCALTIME, 0); }
		public TerminalNode LOCALTIMESTAMP() { return getToken(SqlBaseParser.LOCALTIMESTAMP, 0); }
		public SpecialDateTimeFunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpecialDateTimeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpecialDateTimeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpecialDateTimeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubstringContext extends PrimaryExpressionContext {
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubstring(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubstring(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParameterContext extends PrimaryExpressionContext {
		public ParameterContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParameter(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NormalizeContext extends PrimaryExpressionContext {
		public TerminalNode NORMALIZE() { return getToken(SqlBaseParser.NORMALIZE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public NormalFormContext normalForm() {
			return getRuleContext(NormalFormContext.class,0);
		}
		public NormalizeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalize(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalize(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalLiteralContext extends PrimaryExpressionContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends PrimaryExpressionContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends PrimaryExpressionContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends PrimaryExpressionContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryLiteralContext extends PrimaryExpressionContext {
		public TerminalNode BINARY_LITERAL() { return getToken(SqlBaseParser.BINARY_LITERAL, 0); }
		public BinaryLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBinaryLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBinaryLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBinaryLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentUserContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentUser(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentUser(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExtractContext extends PrimaryExpressionContext {
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends PrimaryExpressionContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayConstructorContext extends PrimaryExpressionContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ArrayConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArrayConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArrayConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArrayConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public FilterContext filter() {
			return getRuleContext(FilterContext.class,0);
		}
		public OverContext over() {
			return getRuleContext(OverContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public NullTreatmentContext nullTreatment() {
			return getRuleContext(NullTreatmentContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends PrimaryExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionContext extends PrimaryExpressionContext {
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GroupingOperationContext extends PrimaryExpressionContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public GroupingOperationContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 92;
		enterRecursionRule(_localctx, 92, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1616);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,209,_ctx) ) {
			case 1:
				{
				_localctx = new NullLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1378);
				match(NULL);
				}
				break;
			case 2:
				{
				_localctx = new IntervalLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1379);
				interval();
				}
				break;
			case 3:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1380);
				identifier();
				setState(1381);
				string();
				}
				break;
			case 4:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1383);
				match(DOUBLE_PRECISION);
				setState(1384);
				string();
				}
				break;
			case 5:
				{
				_localctx = new NumericLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1385);
				number();
				}
				break;
			case 6:
				{
				_localctx = new BooleanLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1386);
				booleanValue();
				}
				break;
			case 7:
				{
				_localctx = new StringLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1387);
				string();
				}
				break;
			case 8:
				{
				_localctx = new BinaryLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1388);
				match(BINARY_LITERAL);
				}
				break;
			case 9:
				{
				_localctx = new ParameterContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1389);
				match(T__4);
				}
				break;
			case 10:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1390);
				match(POSITION);
				setState(1391);
				match(T__1);
				setState(1392);
				valueExpression(0);
				setState(1393);
				match(IN);
				setState(1394);
				valueExpression(0);
				setState(1395);
				match(T__2);
				}
				break;
			case 11:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1397);
				match(T__1);
				setState(1398);
				expression();
				setState(1401); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1399);
					match(T__3);
					setState(1400);
					expression();
					}
					}
					setState(1403); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__3 );
				setState(1405);
				match(T__2);
				}
				break;
			case 12:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1407);
				match(ROW);
				setState(1408);
				match(T__1);
				setState(1409);
				expression();
				setState(1414);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1410);
					match(T__3);
					setState(1411);
					expression();
					}
					}
					setState(1416);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1417);
				match(T__2);
				}
				break;
			case 13:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1419);
				qualifiedName();
				setState(1420);
				match(T__1);
				setState(1421);
				match(ASTERISK);
				setState(1422);
				match(T__2);
				setState(1424);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
				case 1:
					{
					setState(1423);
					filter();
					}
					break;
				}
				setState(1427);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
				case 1:
					{
					setState(1426);
					over();
					}
					break;
				}
				}
				break;
			case 14:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1429);
				qualifiedName();
				setState(1430);
				match(T__1);
				setState(1442);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -6851382923523040220L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -18036704055397633L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 11256903631562495L) != 0)) {
					{
					setState(1432);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
					case 1:
						{
						setState(1431);
						setQuantifier();
						}
						break;
					}
					setState(1434);
					expression();
					setState(1439);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1435);
						match(T__3);
						setState(1436);
						expression();
						}
						}
						setState(1441);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1454);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(1444);
					match(ORDER);
					setState(1445);
					match(BY);
					setState(1446);
					sortItem();
					setState(1451);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1447);
						match(T__3);
						setState(1448);
						sortItem();
						}
						}
						setState(1453);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1456);
				match(T__2);
				setState(1458);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,190,_ctx) ) {
				case 1:
					{
					setState(1457);
					filter();
					}
					break;
				}
				setState(1464);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
				case 1:
					{
					setState(1461);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IGNORE || _la==RESPECT) {
						{
						setState(1460);
						nullTreatment();
						}
					}

					setState(1463);
					over();
					}
					break;
				}
				}
				break;
			case 15:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1466);
				identifier();
				setState(1467);
				match(T__5);
				setState(1468);
				expression();
				}
				break;
			case 16:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1470);
				match(T__1);
				setState(1479);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 2353942828582394880L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2287595198925239029L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 2111062832506607L) != 0)) {
					{
					setState(1471);
					identifier();
					setState(1476);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1472);
						match(T__3);
						setState(1473);
						identifier();
						}
						}
						setState(1478);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1481);
				match(T__2);
				setState(1482);
				match(T__5);
				setState(1483);
				expression();
				}
				break;
			case 17:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1484);
				match(T__1);
				setState(1485);
				query();
				setState(1486);
				match(T__2);
				}
				break;
			case 18:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1488);
				match(EXISTS);
				setState(1489);
				match(T__1);
				setState(1490);
				query();
				setState(1491);
				match(T__2);
				}
				break;
			case 19:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1493);
				match(CASE);
				setState(1494);
				valueExpression(0);
				setState(1496); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1495);
					whenClause();
					}
					}
					setState(1498); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1502);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1500);
					match(ELSE);
					setState(1501);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1504);
				match(END);
				}
				break;
			case 20:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1506);
				match(CASE);
				setState(1508); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1507);
					whenClause();
					}
					}
					setState(1510); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1514);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1512);
					match(ELSE);
					setState(1513);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1516);
				match(END);
				}
				break;
			case 21:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1518);
				match(CAST);
				setState(1519);
				match(T__1);
				setState(1520);
				expression();
				setState(1521);
				match(AS);
				setState(1522);
				type(0);
				setState(1523);
				match(T__2);
				}
				break;
			case 22:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1525);
				match(TRY_CAST);
				setState(1526);
				match(T__1);
				setState(1527);
				expression();
				setState(1528);
				match(AS);
				setState(1529);
				type(0);
				setState(1530);
				match(T__2);
				}
				break;
			case 23:
				{
				_localctx = new ArrayConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1532);
				match(ARRAY);
				setState(1533);
				match(T__6);
				setState(1542);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -6869397322032522204L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -18036704055397633L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 11256903631562495L) != 0)) {
					{
					setState(1534);
					expression();
					setState(1539);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1535);
						match(T__3);
						setState(1536);
						expression();
						}
						}
						setState(1541);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1544);
				match(T__7);
				}
				break;
			case 24:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1545);
				identifier();
				}
				break;
			case 25:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1546);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_DATE);
				}
				break;
			case 26:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1547);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_TIME);
				setState(1551);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,201,_ctx) ) {
				case 1:
					{
					setState(1548);
					match(T__1);
					setState(1549);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(1550);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 27:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1553);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_TIMESTAMP);
				setState(1557);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
				case 1:
					{
					setState(1554);
					match(T__1);
					setState(1555);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(1556);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 28:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1559);
				((SpecialDateTimeFunctionContext)_localctx).name = match(LOCALTIME);
				setState(1563);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
				case 1:
					{
					setState(1560);
					match(T__1);
					setState(1561);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(1562);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 29:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1565);
				((SpecialDateTimeFunctionContext)_localctx).name = match(LOCALTIMESTAMP);
				setState(1569);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,204,_ctx) ) {
				case 1:
					{
					setState(1566);
					match(T__1);
					setState(1567);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(1568);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 30:
				{
				_localctx = new CurrentUserContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1571);
				((CurrentUserContext)_localctx).name = match(CURRENT_USER);
				}
				break;
			case 31:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1572);
				match(SUBSTRING);
				setState(1573);
				match(T__1);
				setState(1574);
				valueExpression(0);
				setState(1575);
				match(FROM);
				setState(1576);
				valueExpression(0);
				setState(1579);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(1577);
					match(FOR);
					setState(1578);
					valueExpression(0);
					}
				}

				setState(1581);
				match(T__2);
				}
				break;
			case 32:
				{
				_localctx = new NormalizeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1583);
				match(NORMALIZE);
				setState(1584);
				match(T__1);
				setState(1585);
				valueExpression(0);
				setState(1588);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(1586);
					match(T__3);
					setState(1587);
					normalForm();
					}
				}

				setState(1590);
				match(T__2);
				}
				break;
			case 33:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1592);
				match(EXTRACT);
				setState(1593);
				match(T__1);
				setState(1594);
				identifier();
				setState(1595);
				match(FROM);
				setState(1596);
				valueExpression(0);
				setState(1597);
				match(T__2);
				}
				break;
			case 34:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1599);
				match(T__1);
				setState(1600);
				expression();
				setState(1601);
				match(T__2);
				}
				break;
			case 35:
				{
				_localctx = new GroupingOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1603);
				match(GROUPING);
				setState(1604);
				match(T__1);
				setState(1613);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 2353942828582394880L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2287595198925239029L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 2111062832506607L) != 0)) {
					{
					setState(1605);
					qualifiedName();
					setState(1610);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1606);
						match(T__3);
						setState(1607);
						qualifiedName();
						}
						}
						setState(1612);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1615);
				match(T__2);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1628);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,211,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1626);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(1618);
						if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
						setState(1619);
						match(T__6);
						setState(1620);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(1621);
						match(T__7);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(1623);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(1624);
						match(T__0);
						setState(1625);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(1630);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,211,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StringContext extends ParserRuleContext {
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
	 
		public StringContext() { }
		public void copyFrom(StringContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnicodeStringLiteralContext extends StringContext {
		public TerminalNode UNICODE_STRING() { return getToken(SqlBaseParser.UNICODE_STRING, 0); }
		public TerminalNode UESCAPE() { return getToken(SqlBaseParser.UESCAPE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public UnicodeStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnicodeStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnicodeStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnicodeStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BasicStringLiteralContext extends StringContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public BasicStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBasicStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBasicStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBasicStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_string);
		try {
			setState(1637);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				_localctx = new BasicStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1631);
				match(STRING);
				}
				break;
			case UNICODE_STRING:
				_localctx = new UnicodeStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1632);
				match(UNICODE_STRING);
				setState(1635);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
				case 1:
					{
					setState(1633);
					match(UESCAPE);
					setState(1634);
					match(STRING);
					}
					break;
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NullTreatmentContext extends ParserRuleContext {
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public NullTreatmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullTreatment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullTreatment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullTreatment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullTreatment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullTreatmentContext nullTreatment() throws RecognitionException {
		NullTreatmentContext _localctx = new NullTreatmentContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_nullTreatment);
		try {
			setState(1643);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IGNORE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1639);
				match(IGNORE);
				setState(1640);
				match(NULLS);
				}
				break;
			case RESPECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1641);
				match(RESPECT);
				setState(1642);
				match(NULLS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneSpecifierContext extends ParserRuleContext {
		public TimeZoneSpecifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeZoneSpecifier; }
	 
		public TimeZoneSpecifierContext() { }
		public void copyFrom(TimeZoneSpecifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneIntervalContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public TimeZoneIntervalContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneInterval(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneStringContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TimeZoneStringContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneString(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneString(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeZoneSpecifierContext timeZoneSpecifier() throws RecognitionException {
		TimeZoneSpecifierContext _localctx = new TimeZoneSpecifierContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_timeZoneSpecifier);
		try {
			setState(1651);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,215,_ctx) ) {
			case 1:
				_localctx = new TimeZoneIntervalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1645);
				match(TIME);
				setState(1646);
				match(ZONE);
				setState(1647);
				interval();
				}
				break;
			case 2:
				_localctx = new TimeZoneStringContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1648);
				match(TIME);
				setState(1649);
				match(ZONE);
				setState(1650);
				string();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1653);
			_la = _input.LA(1);
			if ( !(((((_la - 221)) & ~0x3f) == 0 && ((1L << (_la - 221)) & 63L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonQuantifierContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public ComparisonQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonQuantifierContext comparisonQuantifier() throws RecognitionException {
		ComparisonQuantifierContext _localctx = new ComparisonQuantifierContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_comparisonQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1655);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1657);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalContext extends ParserRuleContext {
		public Token sign;
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_interval);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1659);
			match(INTERVAL);
			setState(1661);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(1660);
				((IntervalContext)_localctx).sign = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1663);
			string();
			setState(1664);
			((IntervalContext)_localctx).from = intervalField();
			setState(1667);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,217,_ctx) ) {
			case 1:
				{
				setState(1665);
				match(TO);
				setState(1666);
				((IntervalContext)_localctx).to = intervalField();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalFieldContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_intervalField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1669);
			_la = _input.LA(1);
			if ( !(_la==DAY || ((((_la - 86)) & ~0x3f) == 0 && ((1L << (_la - 86)) & 1610612737L) != 0) || _la==SECOND || _la==YEAR) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NormalFormContext extends ParserRuleContext {
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public NormalFormContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_normalForm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalForm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalForm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalForm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NormalFormContext normalForm() throws RecognitionException {
		NormalFormContext _localctx = new NormalFormContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_normalForm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1671);
			_la = _input.LA(1);
			if ( !(((((_la - 119)) & ~0x3f) == 0 && ((1L << (_la - 119)) & 15L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TypesContext extends ParserRuleContext {
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public TypesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_types; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypesContext types() throws RecognitionException {
		TypesContext _localctx = new TypesContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_types);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1673);
			match(T__1);
			setState(1682);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 2353942828582394880L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2287595198925239029L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 17873661528303343L) != 0)) {
				{
				setState(1674);
				type(0);
				setState(1679);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1675);
					match(T__3);
					setState(1676);
					type(0);
					}
					}
					setState(1681);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1684);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TypeContext extends ParserRuleContext {
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public BaseTypeContext baseType() {
			return getRuleContext(BaseTypeContext.class,0);
		}
		public List<TypeParameterContext> typeParameter() {
			return getRuleContexts(TypeParameterContext.class);
		}
		public TypeParameterContext typeParameter(int i) {
			return getRuleContext(TypeParameterContext.class,i);
		}
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeContext type() throws RecognitionException {
		return type(0);
	}

	private TypeContext type(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TypeContext _localctx = new TypeContext(_ctx, _parentState);
		TypeContext _prevctx = _localctx;
		int _startState = 114;
		enterRecursionRule(_localctx, 114, RULE_type, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1733);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
			case 1:
				{
				setState(1687);
				match(ARRAY);
				setState(1688);
				match(LT);
				setState(1689);
				type(0);
				setState(1690);
				match(GT);
				}
				break;
			case 2:
				{
				setState(1692);
				match(MAP);
				setState(1693);
				match(LT);
				setState(1694);
				type(0);
				setState(1695);
				match(T__3);
				setState(1696);
				type(0);
				setState(1697);
				match(GT);
				}
				break;
			case 3:
				{
				setState(1699);
				match(ROW);
				setState(1700);
				match(T__1);
				setState(1701);
				identifier();
				setState(1702);
				type(0);
				setState(1709);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1703);
					match(T__3);
					setState(1704);
					identifier();
					setState(1705);
					type(0);
					}
					}
					setState(1711);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1712);
				match(T__2);
				}
				break;
			case 4:
				{
				setState(1714);
				baseType();
				setState(1726);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
				case 1:
					{
					setState(1715);
					match(T__1);
					setState(1716);
					typeParameter();
					setState(1721);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1717);
						match(T__3);
						setState(1718);
						typeParameter();
						}
						}
						setState(1723);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1724);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 5:
				{
				setState(1728);
				match(INTERVAL);
				setState(1729);
				((TypeContext)_localctx).from = intervalField();
				setState(1730);
				match(TO);
				setState(1731);
				((TypeContext)_localctx).to = intervalField();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1739);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,224,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new TypeContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_type);
					setState(1735);
					if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
					setState(1736);
					match(ARRAY);
					}
					} 
				}
				setState(1741);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,224,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TypeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TypeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeParameterContext typeParameter() throws RecognitionException {
		TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_typeParameter);
		try {
			setState(1744);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1742);
				match(INTEGER_VALUE);
				}
				break;
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_ROLE:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case EXTERNAL:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
			case TIME_WITH_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
			case DOUBLE_PRECISION:
				enterOuterAlt(_localctx, 2);
				{
				setState(1743);
				type(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BaseTypeContext extends ParserRuleContext {
		public TerminalNode TIME_WITH_TIME_ZONE() { return getToken(SqlBaseParser.TIME_WITH_TIME_ZONE, 0); }
		public TerminalNode TIMESTAMP_WITH_TIME_ZONE() { return getToken(SqlBaseParser.TIMESTAMP_WITH_TIME_ZONE, 0); }
		public TerminalNode DOUBLE_PRECISION() { return getToken(SqlBaseParser.DOUBLE_PRECISION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public BaseTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_baseType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBaseType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBaseType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBaseType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BaseTypeContext baseType() throws RecognitionException {
		BaseTypeContext _localctx = new BaseTypeContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_baseType);
		try {
			setState(1750);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TIME_WITH_TIME_ZONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1746);
				match(TIME_WITH_TIME_ZONE);
				}
				break;
			case TIMESTAMP_WITH_TIME_ZONE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1747);
				match(TIMESTAMP_WITH_TIME_ZONE);
				}
				break;
			case DOUBLE_PRECISION:
				enterOuterAlt(_localctx, 3);
				{
				setState(1748);
				match(DOUBLE_PRECISION);
				}
				break;
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_ROLE:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case EXTERNAL:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 4);
				{
				setState(1749);
				qualifiedName();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1752);
			match(WHEN);
			setState(1753);
			((WhenClauseContext)_localctx).condition = expression();
			setState(1754);
			match(THEN);
			setState(1755);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FilterContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public FilterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFilter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFilter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFilter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterContext filter() throws RecognitionException {
		FilterContext _localctx = new FilterContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_filter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1757);
			match(FILTER);
			setState(1758);
			match(T__1);
			setState(1759);
			match(WHERE);
			setState(1760);
			booleanExpression(0);
			setState(1761);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OverContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public OverContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_over; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOver(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOver(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOver(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OverContext over() throws RecognitionException {
		OverContext _localctx = new OverContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_over);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1763);
			match(OVER);
			setState(1764);
			match(T__1);
			setState(1775);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(1765);
				match(PARTITION);
				setState(1766);
				match(BY);
				setState(1767);
				((OverContext)_localctx).expression = expression();
				((OverContext)_localctx).partition.add(((OverContext)_localctx).expression);
				setState(1772);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1768);
					match(T__3);
					setState(1769);
					((OverContext)_localctx).expression = expression();
					((OverContext)_localctx).partition.add(((OverContext)_localctx).expression);
					}
					}
					setState(1774);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1787);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1777);
				match(ORDER);
				setState(1778);
				match(BY);
				setState(1779);
				sortItem();
				setState(1784);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1780);
					match(T__3);
					setState(1781);
					sortItem();
					}
					}
					setState(1786);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1790);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUPS || _la==RANGE || _la==ROWS) {
				{
				setState(1789);
				windowFrame();
				}
			}

			setState(1792);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode GROUPS() { return getToken(SqlBaseParser.GROUPS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_windowFrame);
		try {
			setState(1818);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,232,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1794);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(1795);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1796);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(1797);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1798);
				((WindowFrameContext)_localctx).frameType = match(GROUPS);
				setState(1799);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1800);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(1801);
				match(BETWEEN);
				setState(1802);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(1803);
				match(AND);
				setState(1804);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1806);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(1807);
				match(BETWEEN);
				setState(1808);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(1809);
				match(AND);
				setState(1810);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1812);
				((WindowFrameContext)_localctx).frameType = match(GROUPS);
				setState(1813);
				match(BETWEEN);
				setState(1814);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(1815);
				match(AND);
				setState(1816);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FrameBoundContext extends ParserRuleContext {
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
	 
		public FrameBoundContext() { }
		public void copyFrom(FrameBoundContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BoundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public BoundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBoundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBoundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBoundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnboundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public UnboundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnboundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnboundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnboundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRowBoundContext extends FrameBoundContext {
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public CurrentRowBoundContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRowBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRowBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRowBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_frameBound);
		int _la;
		try {
			setState(1829);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,233,_ctx) ) {
			case 1:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1820);
				match(UNBOUNDED);
				setState(1821);
				((UnboundedFrameContext)_localctx).boundType = match(PRECEDING);
				}
				break;
			case 2:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1822);
				match(UNBOUNDED);
				setState(1823);
				((UnboundedFrameContext)_localctx).boundType = match(FOLLOWING);
				}
				break;
			case 3:
				_localctx = new CurrentRowBoundContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1824);
				match(CURRENT);
				setState(1825);
				match(ROW);
				}
				break;
			case 4:
				_localctx = new BoundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1826);
				expression();
				setState(1827);
				((BoundedFrameContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((BoundedFrameContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UpdateAssignmentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public UpdateAssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateAssignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdateAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdateAssignment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUpdateAssignment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateAssignmentContext updateAssignment() throws RecognitionException {
		UpdateAssignmentContext _localctx = new UpdateAssignmentContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_updateAssignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1831);
			identifier();
			setState(1832);
			match(EQ);
			setState(1833);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExplainOptionContext extends ParserRuleContext {
		public ExplainOptionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explainOption; }
	 
		public ExplainOptionContext() { }
		public void copyFrom(ExplainOptionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainFormatContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public ExplainFormatContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainTypeContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode IO() { return getToken(SqlBaseParser.IO, 0); }
		public ExplainTypeContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExplainOptionContext explainOption() throws RecognitionException {
		ExplainOptionContext _localctx = new ExplainOptionContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_explainOption);
		int _la;
		try {
			setState(1839);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FORMAT:
				_localctx = new ExplainFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1835);
				match(FORMAT);
				setState(1836);
				((ExplainFormatContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==GRAPHVIZ || _la==JSON || _la==TEXT) ) {
					((ExplainFormatContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case TYPE:
				_localctx = new ExplainTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1837);
				match(TYPE);
				setState(1838);
				((ExplainTypeContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 55)) & ~0x3f) == 0 && ((1L << (_la - 55)) & 144123984168878081L) != 0) || _la==VALIDATE) ) {
					((ExplainTypeContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TransactionModeContext extends ParserRuleContext {
		public TransactionModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transactionMode; }
	 
		public TransactionModeContext() { }
		public void copyFrom(TransactionModeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TransactionAccessModeContext extends TransactionModeContext {
		public Token accessMode;
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TransactionAccessModeContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransactionAccessMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransactionAccessMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTransactionAccessMode(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IsolationLevelContext extends TransactionModeContext {
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public LevelOfIsolationContext levelOfIsolation() {
			return getRuleContext(LevelOfIsolationContext.class,0);
		}
		public IsolationLevelContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIsolationLevel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIsolationLevel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIsolationLevel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransactionModeContext transactionMode() throws RecognitionException {
		TransactionModeContext _localctx = new TransactionModeContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_transactionMode);
		int _la;
		try {
			setState(1846);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ISOLATION:
				_localctx = new IsolationLevelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1841);
				match(ISOLATION);
				setState(1842);
				match(LEVEL);
				setState(1843);
				levelOfIsolation();
				}
				break;
			case READ:
				_localctx = new TransactionAccessModeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1844);
				match(READ);
				setState(1845);
				((TransactionAccessModeContext)_localctx).accessMode = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ONLY || _la==WRITE) ) {
					((TransactionAccessModeContext)_localctx).accessMode = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LevelOfIsolationContext extends ParserRuleContext {
		public LevelOfIsolationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_levelOfIsolation; }
	 
		public LevelOfIsolationContext() { }
		public void copyFrom(LevelOfIsolationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReadUncommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public ReadUncommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadUncommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadUncommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadUncommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SerializableContext extends LevelOfIsolationContext {
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public SerializableContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSerializable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSerializable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSerializable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReadCommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public ReadCommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadCommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadCommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadCommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepeatableReadContext extends LevelOfIsolationContext {
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public RepeatableReadContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepeatableRead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepeatableRead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepeatableRead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LevelOfIsolationContext levelOfIsolation() throws RecognitionException {
		LevelOfIsolationContext _localctx = new LevelOfIsolationContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_levelOfIsolation);
		try {
			setState(1855);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
			case 1:
				_localctx = new ReadUncommittedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1848);
				match(READ);
				setState(1849);
				match(UNCOMMITTED);
				}
				break;
			case 2:
				_localctx = new ReadCommittedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1850);
				match(READ);
				setState(1851);
				match(COMMITTED);
				}
				break;
			case 3:
				_localctx = new RepeatableReadContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1852);
				match(REPEATABLE);
				setState(1853);
				match(READ);
				}
				break;
			case 4:
				_localctx = new SerializableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1854);
				match(SERIALIZABLE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CallArgumentContext extends ParserRuleContext {
		public CallArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_callArgument; }
	 
		public CallArgumentContext() { }
		public void copyFrom(CallArgumentContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionalArgumentContext extends CallArgumentContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PositionalArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPositionalArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPositionalArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPositionalArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NamedArgumentContext extends CallArgumentContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NamedArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CallArgumentContext callArgument() throws RecognitionException {
		CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_callArgument);
		try {
			setState(1862);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
			case 1:
				_localctx = new PositionalArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1857);
				expression();
				}
				break;
			case 2:
				_localctx = new NamedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1858);
				identifier();
				setState(1859);
				match(T__8);
				setState(1860);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrivilegeContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PrivilegeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilege; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrivilege(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrivilege(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrivilege(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrivilegeContext privilege() throws RecognitionException {
		PrivilegeContext _localctx = new PrivilegeContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_privilege);
		try {
			setState(1868);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				enterOuterAlt(_localctx, 1);
				{
				setState(1864);
				match(SELECT);
				}
				break;
			case DELETE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1865);
				match(DELETE);
				}
				break;
			case INSERT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1866);
				match(INSERT);
				}
				break;
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_ROLE:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case EXTERNAL:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 4);
				{
				setState(1867);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1870);
			identifier();
			setState(1875);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,239,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1871);
					match(T__0);
					setState(1872);
					identifier();
					}
					} 
				}
				setState(1877);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,239,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableVersionExpressionContext extends ParserRuleContext {
		public TableVersionExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableVersionExpression; }
	 
		public TableVersionExpressionContext() { }
		public void copyFrom(TableVersionExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableVersionContext extends TableVersionExpressionContext {
		public Token tableVersionType;
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode SYSTEM_TIME() { return getToken(SqlBaseParser.SYSTEM_TIME, 0); }
		public TerminalNode SYSTEM_VERSION() { return getToken(SqlBaseParser.SYSTEM_VERSION, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TableVersionContext(TableVersionExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableVersion(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableVersion(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableVersion(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableVersionExpressionContext tableVersionExpression() throws RecognitionException {
		TableVersionExpressionContext _localctx = new TableVersionExpressionContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_tableVersionExpression);
		int _la;
		try {
			_localctx = new TableVersionContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(1878);
			match(FOR);
			setState(1879);
			((TableVersionContext)_localctx).tableVersionType = _input.LT(1);
			_la = _input.LA(1);
			if ( !(((((_la - 184)) & ~0x3f) == 0 && ((1L << (_la - 184)) & 268435971L) != 0)) ) {
				((TableVersionContext)_localctx).tableVersionType = (Token)_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1880);
			match(AS);
			setState(1881);
			match(OF);
			setState(1882);
			valueExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GrantorContext extends ParserRuleContext {
		public GrantorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantor; }
	 
		public GrantorContext() { }
		public void copyFrom(GrantorContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentUserGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentUserGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentUserGrantor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentUserGrantor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecifiedPrincipalContext extends GrantorContext {
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public SpecifiedPrincipalContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpecifiedPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpecifiedPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRoleGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_ROLE() { return getToken(SqlBaseParser.CURRENT_ROLE, 0); }
		public CurrentRoleGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRoleGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRoleGrantor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRoleGrantor(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GrantorContext grantor() throws RecognitionException {
		GrantorContext _localctx = new GrantorContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_grantor);
		try {
			setState(1887);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,240,_ctx) ) {
			case 1:
				_localctx = new CurrentUserGrantorContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1884);
				match(CURRENT_USER);
				}
				break;
			case 2:
				_localctx = new CurrentRoleGrantorContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1885);
				match(CURRENT_ROLE);
				}
				break;
			case 3:
				_localctx = new SpecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1886);
				principal();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrincipalContext extends ParserRuleContext {
		public PrincipalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_principal; }
	 
		public PrincipalContext() { }
		public void copyFrom(PrincipalContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnspecifiedPrincipalContext extends PrincipalContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnspecifiedPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnspecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnspecifiedPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnspecifiedPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UserPrincipalContext extends PrincipalContext {
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UserPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUserPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUserPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUserPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RolePrincipalContext extends PrincipalContext {
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RolePrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRolePrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRolePrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRolePrincipal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrincipalContext principal() throws RecognitionException {
		PrincipalContext _localctx = new PrincipalContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_principal);
		try {
			setState(1894);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,241,_ctx) ) {
			case 1:
				_localctx = new UserPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1889);
				match(USER);
				setState(1890);
				identifier();
				}
				break;
			case 2:
				_localctx = new RolePrincipalContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1891);
				match(ROLE);
				setState(1892);
				identifier();
				}
				break;
			case 3:
				_localctx = new UnspecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1893);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RolesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRoles(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RolesContext roles() throws RecognitionException {
		RolesContext _localctx = new RolesContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_roles);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1896);
			identifier();
			setState(1901);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1897);
				match(T__3);
				setState(1898);
				identifier();
				}
				}
				setState(1903);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BackQuotedIdentifierContext extends IdentifierContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public BackQuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBackQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBackQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBackQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends IdentifierContext {
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(SqlBaseParser.QUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DigitIdentifierContext extends IdentifierContext {
		public TerminalNode DIGIT_IDENTIFIER() { return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0); }
		public DigitIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDigitIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDigitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDigitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_identifier);
		try {
			setState(1909);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1904);
				match(IDENTIFIER);
				}
				break;
			case QUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1905);
				match(QUOTED_IDENTIFIER);
				}
				break;
			case ADD:
			case ADMIN:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case CURRENT_ROLE:
			case DATA:
			case DATE:
			case DAY:
			case DEFINER:
			case DESC:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case EXTERNAL:
			case FETCH:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MATERIALIZED:
			case MINUTE:
			case MONTH:
			case NAME:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OF:
			case OFFSET:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case SQL:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case SYSTEM_TIME:
			case SYSTEM_VERSION:
			case TABLES:
			case TABLESAMPLE:
			case TEMPORARY:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UPDATE:
			case USE:
			case USER:
			case VALIDATE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1906);
				nonReserved();
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new BackQuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1907);
				match(BACKQUOTED_IDENTIFIER);
				}
				break;
			case DIGIT_IDENTIFIER:
				_localctx = new DigitIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1908);
				match(DIGIT_IDENTIFIER);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_VALUE() { return getToken(SqlBaseParser.DOUBLE_VALUE, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_number);
		try {
			setState(1914);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DECIMAL_VALUE:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1911);
				match(DECIMAL_VALUE);
				}
				break;
			case DOUBLE_VALUE:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1912);
				match(DOUBLE_VALUE);
				}
				break;
			case INTEGER_VALUE:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1913);
				match(INTEGER_VALUE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public TerminalNode CALLED() { return getToken(SqlBaseParser.CALLED, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode CURRENT_ROLE() { return getToken(SqlBaseParser.CURRENT_ROLE, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATE() { return getToken(SqlBaseParser.DATE, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DETERMINISTIC() { return getToken(SqlBaseParser.DETERMINISTIC, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode GROUPS() { return getToken(SqlBaseParser.GROUPS, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public TerminalNode IO() { return getToken(SqlBaseParser.IO, 0); }
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public TerminalNode LANGUAGE() { return getToken(SqlBaseParser.LANGUAGE, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode NAME() { return getToken(SqlBaseParser.NAME, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NONE() { return getToken(SqlBaseParser.NONE, 0); }
		public TerminalNode NULLIF() { return getToken(SqlBaseParser.NULLIF, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode OFFSET() { return getToken(SqlBaseParser.OFFSET, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode RETURN() { return getToken(SqlBaseParser.RETURN, 0); }
		public TerminalNode RETURNS() { return getToken(SqlBaseParser.RETURNS, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SQL() { return getToken(SqlBaseParser.SQL, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public TerminalNode SYSTEM_TIME() { return getToken(SqlBaseParser.SYSTEM_TIME, 0); }
		public TerminalNode SYSTEM_VERSION() { return getToken(SqlBaseParser.SYSTEM_VERSION, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1916);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 2353942828582394880L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2287595198925239029L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935123922483804783L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 507176687L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 24:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 34:
			return relation_sempred((RelationContext)_localctx, predIndex);
		case 43:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 45:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 46:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 57:
			return type_sempred((TypeContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean relation_sempred(RelationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 2);
		case 4:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 5:
			return precpred(_ctx, 3);
		case 6:
			return precpred(_ctx, 2);
		case 7:
			return precpred(_ctx, 1);
		case 8:
			return precpred(_ctx, 5);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 9:
			return precpred(_ctx, 14);
		case 10:
			return precpred(_ctx, 12);
		}
		return true;
	}
	private boolean type_sempred(TypeContext _localctx, int predIndex) {
		switch (predIndex) {
		case 11:
			return precpred(_ctx, 6);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u00fa\u077f\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0001\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00b5\b\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u00ba\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u00c0\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u00c4\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u00d2\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u00d6\b\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00da\b"+
		"\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00de\b\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00e6"+
		"\b\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00ea\b\u0003\u0001\u0003"+
		"\u0003\u0003\u00ed\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0003\u0003\u00f4\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0005\u0003\u00fb\b\u0003\n\u0003\f\u0003\u00fe"+
		"\t\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0103\b\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u0107\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u010d\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0114\b\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u011d\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0126\b\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u0131\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0138\b\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0003\u0003\u0142\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u0149\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0151\b\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u0159\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u0161\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0005\u0003\u016b\b\u0003\n\u0003\f\u0003\u016e\t\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0003\u0003\u0173\b\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u0178\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u017e\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0187\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u0190\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003"+
		"\u0003\u0195\b\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0199\b\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u01a1\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0003\u0003\u01a8\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u01b5\b\u0003\u0001\u0003\u0003\u0003"+
		"\u01b8\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0005\u0003\u01c0\b\u0003\n\u0003\f\u0003\u01c3\t\u0003\u0003"+
		"\u0003\u01c5\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u01cc\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u01d5\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u01db\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u01e0\b\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u01e4\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u01ec\b\u0003\n\u0003\f\u0003"+
		"\u01ef\t\u0003\u0003\u0003\u01f1\b\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003"+
		"\u0003\u01fb\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u0206"+
		"\b\u0003\n\u0003\f\u0003\u0209\t\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u020e\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003"+
		"\u0213\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003"+
		"\u0219\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0005\u0003\u0220\b\u0003\n\u0003\f\u0003\u0223\t\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0003\u0003\u0228\b\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u022f\b\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u0235\b\u0003\n\u0003\f\u0003"+
		"\u0238\t\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u023c\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0003\u0003\u0240\b\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0248\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u024e\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u0253\b\u0003\n\u0003\f\u0003"+
		"\u0256\t\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u025a\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0003\u0003\u025e\b\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003"+
		"\u0003\u0268\b\u0003\u0001\u0003\u0003\u0003\u026b\b\u0003\u0001\u0003"+
		"\u0001\u0003\u0003\u0003\u026f\b\u0003\u0001\u0003\u0003\u0003\u0272\b"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u0278"+
		"\b\u0003\n\u0003\f\u0003\u027b\t\u0003\u0001\u0003\u0001\u0003\u0003\u0003"+
		"\u027f\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u0294\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u029a\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u02a0\b\u0003\u0003\u0003\u02a2\b"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u02a8"+
		"\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u02ae"+
		"\b\u0003\u0003\u0003\u02b0\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u02b8\b\u0003\u0003\u0003"+
		"\u02ba\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u02cd\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003"+
		"\u02d2\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u02d9\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u02e5\b\u0003\u0003\u0003\u02e7\b\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u02ef"+
		"\b\u0003\u0003\u0003\u02f1\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003"+
		"\u0301\b\u0003\n\u0003\f\u0003\u0304\t\u0003\u0003\u0003\u0306\b\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u030a\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0003\u0003\u030e\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u031e\b\u0003"+
		"\n\u0003\f\u0003\u0321\t\u0003\u0003\u0003\u0323\b\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u0331"+
		"\b\u0003\n\u0003\f\u0003\u0334\t\u0003\u0001\u0003\u0001\u0003\u0003\u0003"+
		"\u0338\b\u0003\u0003\u0003\u033a\b\u0003\u0001\u0004\u0003\u0004\u033d"+
		"\b\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0003\u0005\u0343"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u0348\b\u0005"+
		"\n\u0005\f\u0005\u034b\t\u0005\u0001\u0006\u0001\u0006\u0003\u0006\u034f"+
		"\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0355"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0359\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0003\u0007\u035d\b\u0007\u0001\b\u0001\b\u0001\b\u0001\b"+
		"\u0003\b\u0363\b\b\u0001\t\u0001\t\u0001\t\u0001\t\u0005\t\u0369\b\t\n"+
		"\t\f\t\u036c\t\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\u000b\u0001\u000b\u0001\u000b\u0001\f\u0005\f\u0378\b\f\n\f\f\f\u037b"+
		"\t\f\u0001\r\u0001\r\u0001\r\u0001\r\u0003\r\u0381\b\r\u0001\u000e\u0005"+
		"\u000e\u0384\b\u000e\n\u000e\f\u000e\u0387\t\u000e\u0001\u000f\u0001\u000f"+
		"\u0001\u0010\u0001\u0010\u0003\u0010\u038d\b\u0010\u0001\u0011\u0001\u0011"+
		"\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0395\b\u0012"+
		"\u0001\u0013\u0001\u0013\u0003\u0013\u0399\b\u0013\u0001\u0014\u0001\u0014"+
		"\u0001\u0014\u0003\u0014\u039e\b\u0014\u0001\u0015\u0001\u0015\u0001\u0015"+
		"\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015"+
		"\u0003\u0015\u03a9\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017"+
		"\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0005\u0017\u03b3\b\u0017"+
		"\n\u0017\f\u0017\u03b6\t\u0017\u0003\u0017\u03b8\b\u0017\u0001\u0017\u0001"+
		"\u0017\u0001\u0017\u0003\u0017\u03bd\b\u0017\u0003\u0017\u03bf\b\u0017"+
		"\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017"+
		"\u0001\u0017\u0003\u0017\u03c8\b\u0017\u0003\u0017\u03ca\b\u0017\u0001"+
		"\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0003"+
		"\u0018\u03d2\b\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0003"+
		"\u0018\u03d8\b\u0018\u0001\u0018\u0005\u0018\u03db\b\u0018\n\u0018\f\u0018"+
		"\u03de\t\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019"+
		"\u0001\u0019\u0001\u0019\u0005\u0019\u03e7\b\u0019\n\u0019\f\u0019\u03ea"+
		"\t\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0003\u0019\u03f0"+
		"\b\u0019\u0001\u001a\u0001\u001a\u0003\u001a\u03f4\b\u001a\u0001\u001a"+
		"\u0001\u001a\u0003\u001a\u03f8\b\u001a\u0001\u001b\u0001\u001b\u0003\u001b"+
		"\u03fc\b\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0005\u001b\u0401\b"+
		"\u001b\n\u001b\f\u001b\u0404\t\u001b\u0001\u001b\u0001\u001b\u0001\u001b"+
		"\u0001\u001b\u0005\u001b\u040a\b\u001b\n\u001b\f\u001b\u040d\t\u001b\u0003"+
		"\u001b\u040f\b\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u0413\b\u001b"+
		"\u0001\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u0418\b\u001b\u0001\u001b"+
		"\u0001\u001b\u0003\u001b\u041c\b\u001b\u0001\u001c\u0003\u001c\u041f\b"+
		"\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0005\u001c\u0424\b\u001c\n"+
		"\u001c\f\u001c\u0427\t\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0001\u001d\u0001\u001d\u0005\u001d\u042f\b\u001d\n\u001d\f\u001d"+
		"\u0432\t\u001d\u0003\u001d\u0434\b\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0005\u001d\u043c\b\u001d\n"+
		"\u001d\f\u001d\u043f\t\u001d\u0003\u001d\u0441\b\u001d\u0001\u001d\u0001"+
		"\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0005"+
		"\u001d\u044a\b\u001d\n\u001d\f\u001d\u044d\t\u001d\u0001\u001d\u0001\u001d"+
		"\u0003\u001d\u0451\b\u001d\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e"+
		"\u0005\u001e\u0457\b\u001e\n\u001e\f\u001e\u045a\t\u001e\u0003\u001e\u045c"+
		"\b\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u0460\b\u001e\u0001\u001f"+
		"\u0001\u001f\u0003\u001f\u0464\b\u001f\u0001\u001f\u0001\u001f\u0001\u001f"+
		"\u0001\u001f\u0001\u001f\u0001 \u0001 \u0001!\u0001!\u0003!\u046f\b!\u0001"+
		"!\u0003!\u0472\b!\u0001!\u0001!\u0001!\u0001!\u0001!\u0003!\u0479\b!\u0001"+
		"\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001"+
		"\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0003\"\u048c"+
		"\b\"\u0005\"\u048e\b\"\n\"\f\"\u0491\t\"\u0001#\u0003#\u0494\b#\u0001"+
		"#\u0001#\u0003#\u0498\b#\u0001#\u0001#\u0003#\u049c\b#\u0001#\u0001#\u0003"+
		"#\u04a0\b#\u0003#\u04a2\b#\u0001$\u0001$\u0001$\u0001$\u0001$\u0001$\u0001"+
		"$\u0005$\u04ab\b$\n$\f$\u04ae\t$\u0001$\u0001$\u0003$\u04b2\b$\u0001%"+
		"\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0003%\u04bb\b%\u0001&\u0001"+
		"&\u0001\'\u0001\'\u0003\'\u04c1\b\'\u0001\'\u0001\'\u0003\'\u04c5\b\'"+
		"\u0003\'\u04c7\b\'\u0001(\u0001(\u0001(\u0001(\u0005(\u04cd\b(\n(\f(\u04d0"+
		"\t(\u0001(\u0001(\u0001)\u0001)\u0003)\u04d6\b)\u0001)\u0001)\u0001)\u0001"+
		")\u0001)\u0001)\u0001)\u0001)\u0001)\u0005)\u04e1\b)\n)\f)\u04e4\t)\u0001"+
		")\u0001)\u0001)\u0003)\u04e9\b)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001"+
		")\u0001)\u0001)\u0001)\u0003)\u04f4\b)\u0001*\u0001*\u0001+\u0001+\u0001"+
		"+\u0003+\u04fb\b+\u0001+\u0001+\u0003+\u04ff\b+\u0001+\u0001+\u0001+\u0001"+
		"+\u0001+\u0001+\u0005+\u0507\b+\n+\f+\u050a\t+\u0001,\u0001,\u0001,\u0001"+
		",\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0003,\u0516\b,\u0001,\u0001"+
		",\u0001,\u0001,\u0001,\u0001,\u0003,\u051e\b,\u0001,\u0001,\u0001,\u0001"+
		",\u0001,\u0005,\u0525\b,\n,\f,\u0528\t,\u0001,\u0001,\u0001,\u0003,\u052d"+
		"\b,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0003,\u0535\b,\u0001,\u0001"+
		",\u0001,\u0001,\u0003,\u053b\b,\u0001,\u0001,\u0003,\u053f\b,\u0001,\u0001"+
		",\u0001,\u0003,\u0544\b,\u0001,\u0001,\u0001,\u0003,\u0549\b,\u0001-\u0001"+
		"-\u0001-\u0001-\u0003-\u054f\b-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001"+
		"-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0005-\u055d\b-\n-\f-\u0560"+
		"\t-\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0004.\u057a\b.\u000b.\f.\u057b\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0005.\u0585\b.\n.\f.\u0588\t.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0003.\u0591\b.\u0001.\u0003"+
		".\u0594\b.\u0001.\u0001.\u0001.\u0003.\u0599\b.\u0001.\u0001.\u0001.\u0005"+
		".\u059e\b.\n.\f.\u05a1\t.\u0003.\u05a3\b.\u0001.\u0001.\u0001.\u0001."+
		"\u0001.\u0005.\u05aa\b.\n.\f.\u05ad\t.\u0003.\u05af\b.\u0001.\u0001.\u0003"+
		".\u05b3\b.\u0001.\u0003.\u05b6\b.\u0001.\u0003.\u05b9\b.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0005.\u05c3\b.\n.\f.\u05c6"+
		"\t.\u0003.\u05c8\b.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0004.\u05d9\b.\u000b"+
		".\f.\u05da\u0001.\u0001.\u0003.\u05df\b.\u0001.\u0001.\u0001.\u0001.\u0004"+
		".\u05e5\b.\u000b.\f.\u05e6\u0001.\u0001.\u0003.\u05eb\b.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0005"+
		".\u0602\b.\n.\f.\u0605\t.\u0003.\u0607\b.\u0001.\u0001.\u0001.\u0001."+
		"\u0001.\u0001.\u0001.\u0003.\u0610\b.\u0001.\u0001.\u0001.\u0001.\u0003"+
		".\u0616\b.\u0001.\u0001.\u0001.\u0001.\u0003.\u061c\b.\u0001.\u0001.\u0001"+
		".\u0001.\u0003.\u0622\b.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0003.\u062c\b.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0003.\u0635\b.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0005.\u0649\b.\n.\f.\u064c\t.\u0003.\u064e\b.\u0001.\u0003.\u0651\b"+
		".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0005.\u065b"+
		"\b.\n.\f.\u065e\t.\u0001/\u0001/\u0001/\u0001/\u0003/\u0664\b/\u0003/"+
		"\u0666\b/\u00010\u00010\u00010\u00010\u00030\u066c\b0\u00011\u00011\u0001"+
		"1\u00011\u00011\u00011\u00031\u0674\b1\u00012\u00012\u00013\u00013\u0001"+
		"4\u00014\u00015\u00015\u00035\u067e\b5\u00015\u00015\u00015\u00015\u0003"+
		"5\u0684\b5\u00016\u00016\u00017\u00017\u00018\u00018\u00018\u00018\u0005"+
		"8\u068e\b8\n8\f8\u0691\t8\u00038\u0693\b8\u00018\u00018\u00019\u00019"+
		"\u00019\u00019\u00019\u00019\u00019\u00019\u00019\u00019\u00019\u0001"+
		"9\u00019\u00019\u00019\u00019\u00019\u00019\u00019\u00019\u00019\u0005"+
		"9\u06ac\b9\n9\f9\u06af\t9\u00019\u00019\u00019\u00019\u00019\u00019\u0001"+
		"9\u00059\u06b8\b9\n9\f9\u06bb\t9\u00019\u00019\u00039\u06bf\b9\u00019"+
		"\u00019\u00019\u00019\u00019\u00039\u06c6\b9\u00019\u00019\u00059\u06ca"+
		"\b9\n9\f9\u06cd\t9\u0001:\u0001:\u0003:\u06d1\b:\u0001;\u0001;\u0001;"+
		"\u0001;\u0003;\u06d7\b;\u0001<\u0001<\u0001<\u0001<\u0001<\u0001=\u0001"+
		"=\u0001=\u0001=\u0001=\u0001=\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0005>\u06eb\b>\n>\f>\u06ee\t>\u0003>\u06f0\b>\u0001>\u0001>"+
		"\u0001>\u0001>\u0001>\u0005>\u06f7\b>\n>\f>\u06fa\t>\u0003>\u06fc\b>\u0001"+
		">\u0003>\u06ff\b>\u0001>\u0001>\u0001?\u0001?\u0001?\u0001?\u0001?\u0001"+
		"?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001"+
		"?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0003?\u071b"+
		"\b?\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0003"+
		"@\u0726\b@\u0001A\u0001A\u0001A\u0001A\u0001B\u0001B\u0001B\u0001B\u0003"+
		"B\u0730\bB\u0001C\u0001C\u0001C\u0001C\u0001C\u0003C\u0737\bC\u0001D\u0001"+
		"D\u0001D\u0001D\u0001D\u0001D\u0001D\u0003D\u0740\bD\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0003E\u0747\bE\u0001F\u0001F\u0001F\u0001F\u0003F\u074d"+
		"\bF\u0001G\u0001G\u0001G\u0005G\u0752\bG\nG\fG\u0755\tG\u0001H\u0001H"+
		"\u0001H\u0001H\u0001H\u0001H\u0001I\u0001I\u0001I\u0003I\u0760\bI\u0001"+
		"J\u0001J\u0001J\u0001J\u0001J\u0003J\u0767\bJ\u0001K\u0001K\u0001K\u0005"+
		"K\u076c\bK\nK\fK\u076f\tK\u0001L\u0001L\u0001L\u0001L\u0001L\u0003L\u0776"+
		"\bL\u0001M\u0001M\u0001M\u0003M\u077b\bM\u0001N\u0001N\u0001N\u0000\u0006"+
		"0DVZ\\rO\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018"+
		"\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080"+
		"\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098"+
		"\u009a\u009c\u0000\u0018\u0002\u0000\u001a\u001a\u009d\u009d\u0002\u0000"+
		"11aa\u0002\u0000JJYY\u0002\u0000==ZZ\u0001\u0000\u00a6\u00a7\u0002\u0000"+
		"\f\f\u00ec\u00ec\u0002\u0000<<\u00cb\u00cb\u0002\u0000\u0013\u001333\u0002"+
		"\u0000FFhh\u0002\u0000\f\f66\u0002\u0000\u0015\u0015\u00b7\u00b7\u0001"+
		"\u0000\u00e3\u00e4\u0001\u0000\u00e5\u00e7\u0001\u0000\u00dd\u00e2\u0003"+
		"\u0000\f\f\u0010\u0010\u00b2\u00b2\u0002\u0000CC\u00c4\u00c4\u0005\u0000"+
		"//VVst\u00aa\u00aa\u00db\u00db\u0001\u0000wz\u0002\u0000GG\u0090\u0090"+
		"\u0003\u0000QQee\u00be\u00be\u0004\u000077bbpp\u00d1\u00d1\u0002\u0000"+
		"\u0085\u0085\u00da\u00da\u0003\u0000\u00b8\u00b9\u00c1\u00c1\u00d4\u00d4"+
		"0\u0000\n\f\u000e\u000e\u0010\u0011\u0013\u0015\u0018\u001a\u001d\"\'"+
		"\'))-/11335577==@@BBDGIILQTTVXZZ\\\\__abdegikkmmpuw|\u0080\u0083\u0085"+
		"\u0086\u0089\u0089\u008b\u0090\u0092\u0095\u0097\u00a0\u00a2\u00a4\u00a6"+
		"\u00ab\u00ad\u00b9\u00bb\u00be\u00c0\u00c3\u00c5\u00c7\u00c9\u00ca\u00cd"+
		"\u00cf\u00d1\u00d1\u00d3\u00d5\u00d9\u00dc\u08ad\u0000\u009e\u0001\u0000"+
		"\u0000\u0000\u0002\u00a1\u0001\u0000\u0000\u0000\u0004\u00a4\u0001\u0000"+
		"\u0000\u0000\u0006\u0339\u0001\u0000\u0000\u0000\b\u033c\u0001\u0000\u0000"+
		"\u0000\n\u0340\u0001\u0000\u0000\u0000\f\u034e\u0001\u0000\u0000\u0000"+
		"\u000e\u0350\u0001\u0000\u0000\u0000\u0010\u035e\u0001\u0000\u0000\u0000"+
		"\u0012\u0364\u0001\u0000\u0000\u0000\u0014\u036f\u0001\u0000\u0000\u0000"+
		"\u0016\u0373\u0001\u0000\u0000\u0000\u0018\u0379\u0001\u0000\u0000\u0000"+
		"\u001a\u0380\u0001\u0000\u0000\u0000\u001c\u0385\u0001\u0000\u0000\u0000"+
		"\u001e\u0388\u0001\u0000\u0000\u0000 \u038c\u0001\u0000\u0000\u0000\""+
		"\u038e\u0001\u0000\u0000\u0000$\u0391\u0001\u0000\u0000\u0000&\u0398\u0001"+
		"\u0000\u0000\u0000(\u039d\u0001\u0000\u0000\u0000*\u03a8\u0001\u0000\u0000"+
		"\u0000,\u03aa\u0001\u0000\u0000\u0000.\u03ac\u0001\u0000\u0000\u00000"+
		"\u03cb\u0001\u0000\u0000\u00002\u03ef\u0001\u0000\u0000\u00004\u03f1\u0001"+
		"\u0000\u0000\u00006\u03f9\u0001\u0000\u0000\u00008\u041e\u0001\u0000\u0000"+
		"\u0000:\u0450\u0001\u0000\u0000\u0000<\u045f\u0001\u0000\u0000\u0000>"+
		"\u0461\u0001\u0000\u0000\u0000@\u046a\u0001\u0000\u0000\u0000B\u0478\u0001"+
		"\u0000\u0000\u0000D\u047a\u0001\u0000\u0000\u0000F\u04a1\u0001\u0000\u0000"+
		"\u0000H\u04b1\u0001\u0000\u0000\u0000J\u04b3\u0001\u0000\u0000\u0000L"+
		"\u04bc\u0001\u0000\u0000\u0000N\u04be\u0001\u0000\u0000\u0000P\u04c8\u0001"+
		"\u0000\u0000\u0000R\u04f3\u0001\u0000\u0000\u0000T\u04f5\u0001\u0000\u0000"+
		"\u0000V\u04fe\u0001\u0000\u0000\u0000X\u0548\u0001\u0000\u0000\u0000Z"+
		"\u054e\u0001\u0000\u0000\u0000\\\u0650\u0001\u0000\u0000\u0000^\u0665"+
		"\u0001\u0000\u0000\u0000`\u066b\u0001\u0000\u0000\u0000b\u0673\u0001\u0000"+
		"\u0000\u0000d\u0675\u0001\u0000\u0000\u0000f\u0677\u0001\u0000\u0000\u0000"+
		"h\u0679\u0001\u0000\u0000\u0000j\u067b\u0001\u0000\u0000\u0000l\u0685"+
		"\u0001\u0000\u0000\u0000n\u0687\u0001\u0000\u0000\u0000p\u0689\u0001\u0000"+
		"\u0000\u0000r\u06c5\u0001\u0000\u0000\u0000t\u06d0\u0001\u0000\u0000\u0000"+
		"v\u06d6\u0001\u0000\u0000\u0000x\u06d8\u0001\u0000\u0000\u0000z\u06dd"+
		"\u0001\u0000\u0000\u0000|\u06e3\u0001\u0000\u0000\u0000~\u071a\u0001\u0000"+
		"\u0000\u0000\u0080\u0725\u0001\u0000\u0000\u0000\u0082\u0727\u0001\u0000"+
		"\u0000\u0000\u0084\u072f\u0001\u0000\u0000\u0000\u0086\u0736\u0001\u0000"+
		"\u0000\u0000\u0088\u073f\u0001\u0000\u0000\u0000\u008a\u0746\u0001\u0000"+
		"\u0000\u0000\u008c\u074c\u0001\u0000\u0000\u0000\u008e\u074e\u0001\u0000"+
		"\u0000\u0000\u0090\u0756\u0001\u0000\u0000\u0000\u0092\u075f\u0001\u0000"+
		"\u0000\u0000\u0094\u0766\u0001\u0000\u0000\u0000\u0096\u0768\u0001\u0000"+
		"\u0000\u0000\u0098\u0775\u0001\u0000\u0000\u0000\u009a\u077a\u0001\u0000"+
		"\u0000\u0000\u009c\u077c\u0001\u0000\u0000\u0000\u009e\u009f\u0003\u0006"+
		"\u0003\u0000\u009f\u00a0\u0005\u0000\u0000\u0001\u00a0\u0001\u0001\u0000"+
		"\u0000\u0000\u00a1\u00a2\u0003T*\u0000\u00a2\u00a3\u0005\u0000\u0000\u0001"+
		"\u00a3\u0003\u0001\u0000\u0000\u0000\u00a4\u00a5\u0003 \u0010\u0000\u00a5"+
		"\u00a6\u0005\u0000\u0000\u0001\u00a6\u0005\u0001\u0000\u0000\u0000\u00a7"+
		"\u033a\u0003\b\u0004\u0000\u00a8\u00a9\u0005\u00ce\u0000\u0000\u00a9\u033a"+
		"\u0003\u0098L\u0000\u00aa\u00ab\u0005\u00ce\u0000\u0000\u00ab\u00ac\u0003"+
		"\u0098L\u0000\u00ac\u00ad\u0005\u0001\u0000\u0000\u00ad\u00ae\u0003\u0098"+
		"L\u0000\u00ae\u033a\u0001\u0000\u0000\u0000\u00af\u00b0\u0005$\u0000\u0000"+
		"\u00b0\u00b4\u0005\u00a8\u0000\u0000\u00b1\u00b2\u0005W\u0000\u0000\u00b2"+
		"\u00b3\u0005~\u0000\u0000\u00b3\u00b5\u0005?\u0000\u0000\u00b4\u00b1\u0001"+
		"\u0000\u0000\u0000\u00b4\u00b5\u0001\u0000\u0000\u0000\u00b5\u00b6\u0001"+
		"\u0000\u0000\u0000\u00b6\u00b9\u0003\u008eG\u0000\u00b7\u00b8\u0005\u00d8"+
		"\u0000\u0000\u00b8\u00ba\u0003\u0012\t\u0000\u00b9\u00b7\u0001\u0000\u0000"+
		"\u0000\u00b9\u00ba\u0001\u0000\u0000\u0000\u00ba\u033a\u0001\u0000\u0000"+
		"\u0000\u00bb\u00bc\u00058\u0000\u0000\u00bc\u00bf\u0005\u00a8\u0000\u0000"+
		"\u00bd\u00be\u0005W\u0000\u0000\u00be\u00c0\u0005?\u0000\u0000\u00bf\u00bd"+
		"\u0001\u0000\u0000\u0000\u00bf\u00c0\u0001\u0000\u0000\u0000\u00c0\u00c1"+
		"\u0001\u0000\u0000\u0000\u00c1\u00c3\u0003\u008eG\u0000\u00c2\u00c4\u0007"+
		"\u0000\u0000\u0000\u00c3\u00c2\u0001\u0000\u0000\u0000\u00c3\u00c4\u0001"+
		"\u0000\u0000\u0000\u00c4\u033a\u0001\u0000\u0000\u0000\u00c5\u00c6\u0005"+
		"\r\u0000\u0000\u00c6\u00c7\u0005\u00a8\u0000\u0000\u00c7\u00c8\u0003\u008e"+
		"G\u0000\u00c8\u00c9\u0005\u0098\u0000\u0000\u00c9\u00ca\u0005\u00c2\u0000"+
		"\u0000\u00ca\u00cb\u0003\u0098L\u0000\u00cb\u033a\u0001\u0000\u0000\u0000"+
		"\u00cc\u00cd\u0005$\u0000\u0000\u00cd\u00d1\u0005\u00ba\u0000\u0000\u00ce"+
		"\u00cf\u0005W\u0000\u0000\u00cf\u00d0\u0005~\u0000\u0000\u00d0\u00d2\u0005"+
		"?\u0000\u0000\u00d1\u00ce\u0001\u0000\u0000\u0000\u00d1\u00d2\u0001\u0000"+
		"\u0000\u0000\u00d2\u00d3\u0001\u0000\u0000\u0000\u00d3\u00d5\u0003\u008e"+
		"G\u0000\u00d4\u00d6\u0003P(\u0000\u00d5\u00d4\u0001\u0000\u0000\u0000"+
		"\u00d5\u00d6\u0001\u0000\u0000\u0000\u00d6\u00d9\u0001\u0000\u0000\u0000"+
		"\u00d7\u00d8\u0005 \u0000\u0000\u00d8\u00da\u0003^/\u0000\u00d9\u00d7"+
		"\u0001\u0000\u0000\u0000\u00d9\u00da\u0001\u0000\u0000\u0000\u00da\u00dd"+
		"\u0001\u0000\u0000\u0000\u00db\u00dc\u0005\u00d8\u0000\u0000\u00dc\u00de"+
		"\u0003\u0012\t\u0000\u00dd\u00db\u0001\u0000\u0000\u0000\u00dd\u00de\u0001"+
		"\u0000\u0000\u0000\u00de\u00df\u0001\u0000\u0000\u0000\u00df\u00e5\u0005"+
		"\u0012\u0000\u0000\u00e0\u00e6\u0003\b\u0004\u0000\u00e1\u00e2\u0005\u0002"+
		"\u0000\u0000\u00e2\u00e3\u0003\b\u0004\u0000\u00e3\u00e4\u0005\u0003\u0000"+
		"\u0000\u00e4\u00e6\u0001\u0000\u0000\u0000\u00e5\u00e0\u0001\u0000\u0000"+
		"\u0000\u00e5\u00e1\u0001\u0000\u0000\u0000\u00e6\u00ec\u0001\u0000\u0000"+
		"\u0000\u00e7\u00e9\u0005\u00d8\u0000\u0000\u00e8\u00ea\u0005{\u0000\u0000"+
		"\u00e9\u00e8\u0001\u0000\u0000\u0000\u00e9\u00ea\u0001\u0000\u0000\u0000"+
		"\u00ea\u00eb\u0001\u0000\u0000\u0000\u00eb\u00ed\u0005-\u0000\u0000\u00ec"+
		"\u00e7\u0001\u0000\u0000\u0000\u00ec\u00ed\u0001\u0000\u0000\u0000\u00ed"+
		"\u033a\u0001\u0000\u0000\u0000\u00ee\u00ef\u0005$\u0000\u0000\u00ef\u00f3"+
		"\u0005\u00ba\u0000\u0000\u00f0\u00f1\u0005W\u0000\u0000\u00f1\u00f2\u0005"+
		"~\u0000\u0000\u00f2\u00f4\u0005?\u0000\u0000\u00f3\u00f0\u0001\u0000\u0000"+
		"\u0000\u00f3\u00f4\u0001\u0000\u0000\u0000\u00f4\u00f5\u0001\u0000\u0000"+
		"\u0000\u00f5\u00f6\u0003\u008eG\u0000\u00f6\u00f7\u0005\u0002\u0000\u0000"+
		"\u00f7\u00fc\u0003\f\u0006\u0000\u00f8\u00f9\u0005\u0004\u0000\u0000\u00f9"+
		"\u00fb\u0003\f\u0006\u0000\u00fa\u00f8\u0001\u0000\u0000\u0000\u00fb\u00fe"+
		"\u0001\u0000\u0000\u0000\u00fc\u00fa\u0001\u0000\u0000\u0000\u00fc\u00fd"+
		"\u0001\u0000\u0000\u0000\u00fd\u00ff\u0001\u0000\u0000\u0000\u00fe\u00fc"+
		"\u0001\u0000\u0000\u0000\u00ff\u0102\u0005\u0003\u0000\u0000\u0100\u0101"+
		"\u0005 \u0000\u0000\u0101\u0103\u0003^/\u0000\u0102\u0100\u0001\u0000"+
		"\u0000\u0000\u0102\u0103\u0001\u0000\u0000\u0000\u0103\u0106\u0001\u0000"+
		"\u0000\u0000\u0104\u0105\u0005\u00d8\u0000\u0000\u0105\u0107\u0003\u0012"+
		"\t\u0000\u0106\u0104\u0001\u0000\u0000\u0000\u0106\u0107\u0001\u0000\u0000"+
		"\u0000\u0107\u033a\u0001\u0000\u0000\u0000\u0108\u0109\u00058\u0000\u0000"+
		"\u0109\u010c\u0005\u00ba\u0000\u0000\u010a\u010b\u0005W\u0000\u0000\u010b"+
		"\u010d\u0005?\u0000\u0000\u010c\u010a\u0001\u0000\u0000\u0000\u010c\u010d"+
		"\u0001\u0000\u0000\u0000\u010d\u010e\u0001\u0000\u0000\u0000\u010e\u033a"+
		"\u0003\u008eG\u0000\u010f\u0110\u0005]\u0000\u0000\u0110\u0111\u0005`"+
		"\u0000\u0000\u0111\u0113\u0003\u008eG\u0000\u0112\u0114\u0003P(\u0000"+
		"\u0113\u0112\u0001\u0000\u0000\u0000\u0113\u0114\u0001\u0000\u0000\u0000"+
		"\u0114\u0115\u0001\u0000\u0000\u0000\u0115\u0116\u0003\b\u0004\u0000\u0116"+
		"\u033a\u0001\u0000\u0000\u0000\u0117\u0118\u00052\u0000\u0000\u0118\u0119"+
		"\u0005J\u0000\u0000\u0119\u011c\u0003\u008eG\u0000\u011a\u011b\u0005\u00d7"+
		"\u0000\u0000\u011b\u011d\u0003V+\u0000\u011c\u011a\u0001\u0000\u0000\u0000"+
		"\u011c\u011d\u0001\u0000\u0000\u0000\u011d\u033a\u0001\u0000\u0000\u0000"+
		"\u011e\u011f\u0005\u00c5\u0000\u0000\u011f\u0120\u0005\u00ba\u0000\u0000"+
		"\u0120\u033a\u0003\u008eG\u0000\u0121\u0122\u0005\r\u0000\u0000\u0122"+
		"\u0125\u0005\u00ba\u0000\u0000\u0123\u0124\u0005W\u0000\u0000\u0124\u0126"+
		"\u0005?\u0000\u0000\u0125\u0123\u0001\u0000\u0000\u0000\u0125\u0126\u0001"+
		"\u0000\u0000\u0000\u0126\u0127\u0001\u0000\u0000\u0000\u0127\u0128\u0003"+
		"\u008eG\u0000\u0128\u0129\u0005\u0098\u0000\u0000\u0129\u012a\u0005\u00c2"+
		"\u0000\u0000\u012a\u012b\u0003\u008eG\u0000\u012b\u033a\u0001\u0000\u0000"+
		"\u0000\u012c\u012d\u0005\r\u0000\u0000\u012d\u0130\u0005\u00ba\u0000\u0000"+
		"\u012e\u012f\u0005W\u0000\u0000\u012f\u0131\u0005?\u0000\u0000\u0130\u012e"+
		"\u0001\u0000\u0000\u0000\u0130\u0131\u0001\u0000\u0000\u0000\u0131\u0132"+
		"\u0001\u0000\u0000\u0000\u0132\u0133\u0003\u008eG\u0000\u0133\u0134\u0005"+
		"\u0098\u0000\u0000\u0134\u0137\u0005\u001e\u0000\u0000\u0135\u0136\u0005"+
		"W\u0000\u0000\u0136\u0138\u0005?\u0000\u0000\u0137\u0135\u0001\u0000\u0000"+
		"\u0000\u0137\u0138\u0001\u0000\u0000\u0000\u0138\u0139\u0001\u0000\u0000"+
		"\u0000\u0139\u013a\u0003\u0098L\u0000\u013a\u013b\u0005\u00c2\u0000\u0000"+
		"\u013b\u013c\u0003\u0098L\u0000\u013c\u033a\u0001\u0000\u0000\u0000\u013d"+
		"\u013e\u0005\r\u0000\u0000\u013e\u0141\u0005\u00ba\u0000\u0000\u013f\u0140"+
		"\u0005W\u0000\u0000\u0140\u0142\u0005?\u0000\u0000\u0141\u013f\u0001\u0000"+
		"\u0000\u0000\u0141\u0142\u0001\u0000\u0000\u0000\u0142\u0143\u0001\u0000"+
		"\u0000\u0000\u0143\u0144\u0003\u008eG\u0000\u0144\u0145\u00058\u0000\u0000"+
		"\u0145\u0148\u0005\u001e\u0000\u0000\u0146\u0147\u0005W\u0000\u0000\u0147"+
		"\u0149\u0005?\u0000\u0000\u0148\u0146\u0001\u0000\u0000\u0000\u0148\u0149"+
		"\u0001\u0000\u0000\u0000\u0149\u014a\u0001\u0000\u0000\u0000\u014a\u014b"+
		"\u0003\u008eG\u0000\u014b\u033a\u0001\u0000\u0000\u0000\u014c\u014d\u0005"+
		"\r\u0000\u0000\u014d\u0150\u0005\u00ba\u0000\u0000\u014e\u014f\u0005W"+
		"\u0000\u0000\u014f\u0151\u0005?\u0000\u0000\u0150\u014e\u0001\u0000\u0000"+
		"\u0000\u0150\u0151\u0001\u0000\u0000\u0000\u0151\u0152\u0001\u0000\u0000"+
		"\u0000\u0152\u0153\u0003\u008eG\u0000\u0153\u0154\u0005\n\u0000\u0000"+
		"\u0154\u0158\u0005\u001e\u0000\u0000\u0155\u0156\u0005W\u0000\u0000\u0156"+
		"\u0157\u0005~\u0000\u0000\u0157\u0159\u0005?\u0000\u0000\u0158\u0155\u0001"+
		"\u0000\u0000\u0000\u0158\u0159\u0001\u0000\u0000\u0000\u0159\u015a\u0001"+
		"\u0000\u0000\u0000\u015a\u015b\u0003\u000e\u0007\u0000\u015b\u033a\u0001"+
		"\u0000\u0000\u0000\u015c\u015d\u0005\u000e\u0000\u0000\u015d\u0160\u0003"+
		"\u008eG\u0000\u015e\u015f\u0005\u00d8\u0000\u0000\u015f\u0161\u0003\u0012"+
		"\t\u0000\u0160\u015e\u0001\u0000\u0000\u0000\u0160\u0161\u0001\u0000\u0000"+
		"\u0000\u0161\u033a\u0001\u0000\u0000\u0000\u0162\u0163\u0005$\u0000\u0000"+
		"\u0163\u0164\u0005\u00c7\u0000\u0000\u0164\u0165\u0003\u008eG\u0000\u0165"+
		"\u0172\u0005\u0012\u0000\u0000\u0166\u0167\u0005\u0002\u0000\u0000\u0167"+
		"\u016c\u0003\u0016\u000b\u0000\u0168\u0169\u0005\u0004\u0000\u0000\u0169"+
		"\u016b\u0003\u0016\u000b\u0000\u016a\u0168\u0001\u0000\u0000\u0000\u016b"+
		"\u016e\u0001\u0000\u0000\u0000\u016c\u016a\u0001\u0000\u0000\u0000\u016c"+
		"\u016d\u0001\u0000\u0000\u0000\u016d\u016f\u0001\u0000\u0000\u0000\u016e"+
		"\u016c\u0001\u0000\u0000\u0000\u016f\u0170\u0005\u0003\u0000\u0000\u0170"+
		"\u0173\u0001\u0000\u0000\u0000\u0171\u0173\u0003r9\u0000\u0172\u0166\u0001"+
		"\u0000\u0000\u0000\u0172\u0171\u0001\u0000\u0000\u0000\u0173\u033a\u0001"+
		"\u0000\u0000\u0000\u0174\u0177\u0005$\u0000\u0000\u0175\u0176\u0005\u0087"+
		"\u0000\u0000\u0176\u0178\u0005\u009a\u0000\u0000\u0177\u0175\u0001\u0000"+
		"\u0000\u0000\u0177\u0178\u0001\u0000\u0000\u0000\u0178\u0179\u0001\u0000"+
		"\u0000\u0000\u0179\u017a\u0005\u00d5\u0000\u0000\u017a\u017d\u0003\u008e"+
		"G\u0000\u017b\u017c\u0005\u00ab\u0000\u0000\u017c\u017e\u0007\u0001\u0000"+
		"\u0000\u017d\u017b\u0001\u0000\u0000\u0000\u017d\u017e\u0001\u0000\u0000"+
		"\u0000\u017e\u017f\u0001\u0000\u0000\u0000\u017f\u0180\u0005\u0012\u0000"+
		"\u0000\u0180\u0181\u0003\b\u0004\u0000\u0181\u033a\u0001\u0000\u0000\u0000"+
		"\u0182\u0183\u00058\u0000\u0000\u0183\u0186\u0005\u00d5\u0000\u0000\u0184"+
		"\u0185\u0005W\u0000\u0000\u0185\u0187\u0005?\u0000\u0000\u0186\u0184\u0001"+
		"\u0000\u0000\u0000\u0186\u0187\u0001\u0000\u0000\u0000\u0187\u0188\u0001"+
		"\u0000\u0000\u0000\u0188\u033a\u0003\u008eG\u0000\u0189\u018a\u0005$\u0000"+
		"\u0000\u018a\u018b\u0005r\u0000\u0000\u018b\u018f\u0005\u00d5\u0000\u0000"+
		"\u018c\u018d\u0005W\u0000\u0000\u018d\u018e\u0005~\u0000\u0000\u018e\u0190"+
		"\u0005?\u0000\u0000\u018f\u018c\u0001\u0000\u0000\u0000\u018f\u0190\u0001"+
		"\u0000\u0000\u0000\u0190\u0191\u0001\u0000\u0000\u0000\u0191\u0194\u0003"+
		"\u008eG\u0000\u0192\u0193\u0005 \u0000\u0000\u0193\u0195\u0003^/\u0000"+
		"\u0194\u0192\u0001\u0000\u0000\u0000\u0194\u0195\u0001\u0000\u0000\u0000"+
		"\u0195\u0198\u0001\u0000\u0000\u0000\u0196\u0197\u0005\u00d8\u0000\u0000"+
		"\u0197\u0199\u0003\u0012\t\u0000\u0198\u0196\u0001\u0000\u0000\u0000\u0198"+
		"\u0199\u0001\u0000\u0000\u0000\u0199\u019a\u0001\u0000\u0000\u0000\u019a"+
		"\u01a0\u0005\u0012\u0000\u0000\u019b\u01a1\u0003\b\u0004\u0000\u019c\u019d"+
		"\u0005\u0002\u0000\u0000\u019d\u019e\u0003\b\u0004\u0000\u019e\u019f\u0005"+
		"\u0003\u0000\u0000\u019f\u01a1\u0001\u0000\u0000\u0000\u01a0\u019b\u0001"+
		"\u0000\u0000\u0000\u01a0\u019c\u0001\u0000\u0000\u0000\u01a1\u033a\u0001"+
		"\u0000\u0000\u0000\u01a2\u01a3\u00058\u0000\u0000\u01a3\u01a4\u0005r\u0000"+
		"\u0000\u01a4\u01a7\u0005\u00d5\u0000\u0000\u01a5\u01a6\u0005W\u0000\u0000"+
		"\u01a6\u01a8\u0005?\u0000\u0000\u01a7\u01a5\u0001\u0000\u0000\u0000\u01a7"+
		"\u01a8\u0001\u0000\u0000\u0000\u01a8\u01a9\u0001\u0000\u0000\u0000\u01a9"+
		"\u033a\u0003\u008eG\u0000\u01aa\u01ab\u0005\u0097\u0000\u0000\u01ab\u01ac"+
		"\u0005r\u0000\u0000\u01ac\u01ad\u0005\u00d5\u0000\u0000\u01ad\u01ae\u0003"+
		"\u008eG\u0000\u01ae\u01af\u0005\u00d7\u0000\u0000\u01af\u01b0\u0003V+"+
		"\u0000\u01b0\u033a\u0001\u0000\u0000\u0000\u01b1\u01b4\u0005$\u0000\u0000"+
		"\u01b2\u01b3\u0005\u0087\u0000\u0000\u01b3\u01b5\u0005\u009a\u0000\u0000"+
		"\u01b4\u01b2\u0001\u0000\u0000\u0000\u01b4\u01b5\u0001\u0000\u0000\u0000"+
		"\u01b5\u01b7\u0001\u0000\u0000\u0000\u01b6\u01b8\u0005\u00bd\u0000\u0000"+
		"\u01b7\u01b6\u0001\u0000\u0000\u0000\u01b7\u01b8\u0001\u0000\u0000\u0000"+
		"\u01b8\u01b9\u0001\u0000\u0000\u0000\u01b9\u01ba\u0005L\u0000\u0000\u01ba"+
		"\u01bb\u0003\u008eG\u0000\u01bb\u01c4\u0005\u0002\u0000\u0000\u01bc\u01c1"+
		"\u0003\u0016\u000b\u0000\u01bd\u01be\u0005\u0004\u0000\u0000\u01be\u01c0"+
		"\u0003\u0016\u000b\u0000\u01bf\u01bd\u0001\u0000\u0000\u0000\u01c0\u01c3"+
		"\u0001\u0000\u0000\u0000\u01c1\u01bf\u0001\u0000\u0000\u0000\u01c1\u01c2"+
		"\u0001\u0000\u0000\u0000\u01c2\u01c5\u0001\u0000\u0000\u0000\u01c3\u01c1"+
		"\u0001\u0000\u0000\u0000\u01c4\u01bc\u0001\u0000\u0000\u0000\u01c4\u01c5"+
		"\u0001\u0000\u0000\u0000\u01c5\u01c6\u0001\u0000\u0000\u0000\u01c6\u01c7"+
		"\u0005\u0003\u0000\u0000\u01c7\u01c8\u0005\u009f\u0000\u0000\u01c8\u01cb"+
		"\u0003r9\u0000\u01c9\u01ca\u0005 \u0000\u0000\u01ca\u01cc\u0003^/\u0000"+
		"\u01cb\u01c9\u0001\u0000\u0000\u0000\u01cb\u01cc\u0001\u0000\u0000\u0000"+
		"\u01cc\u01cd\u0001\u0000\u0000\u0000\u01cd\u01ce\u0003\u0018\f\u0000\u01ce"+
		"\u01cf\u0003 \u0010\u0000\u01cf\u033a\u0001\u0000\u0000\u0000\u01d0\u01d1"+
		"\u0005\r\u0000\u0000\u01d1\u01d2\u0005L\u0000\u0000\u01d2\u01d4\u0003"+
		"\u008eG\u0000\u01d3\u01d5\u0003p8\u0000\u01d4\u01d3\u0001\u0000\u0000"+
		"\u0000\u01d4\u01d5\u0001\u0000\u0000\u0000\u01d5\u01d6\u0001\u0000\u0000"+
		"\u0000\u01d6\u01d7\u0003\u001c\u000e\u0000\u01d7\u033a\u0001\u0000\u0000"+
		"\u0000\u01d8\u01da\u00058\u0000\u0000\u01d9\u01db\u0005\u00bd\u0000\u0000"+
		"\u01da\u01d9\u0001\u0000\u0000\u0000\u01da\u01db\u0001\u0000\u0000\u0000"+
		"\u01db\u01dc\u0001\u0000\u0000\u0000\u01dc\u01df\u0005L\u0000\u0000\u01dd"+
		"\u01de\u0005W\u0000\u0000\u01de\u01e0\u0005?\u0000\u0000\u01df\u01dd\u0001"+
		"\u0000\u0000\u0000\u01df\u01e0\u0001\u0000\u0000\u0000\u01e0\u01e1\u0001"+
		"\u0000\u0000\u0000\u01e1\u01e3\u0003\u008eG\u0000\u01e2\u01e4\u0003p8"+
		"\u0000\u01e3\u01e2\u0001\u0000\u0000\u0000\u01e3\u01e4\u0001\u0000\u0000"+
		"\u0000\u01e4\u033a\u0001\u0000\u0000\u0000\u01e5\u01e6\u0005\u0018\u0000"+
		"\u0000\u01e6\u01e7\u0003\u008eG\u0000\u01e7\u01f0\u0005\u0002\u0000\u0000"+
		"\u01e8\u01ed\u0003\u008aE\u0000\u01e9\u01ea\u0005\u0004\u0000\u0000\u01ea"+
		"\u01ec\u0003\u008aE\u0000\u01eb\u01e9\u0001\u0000\u0000\u0000\u01ec\u01ef"+
		"\u0001\u0000\u0000\u0000\u01ed\u01eb\u0001\u0000\u0000\u0000\u01ed\u01ee"+
		"\u0001\u0000\u0000\u0000\u01ee\u01f1\u0001\u0000\u0000\u0000\u01ef\u01ed"+
		"\u0001\u0000\u0000\u0000\u01f0\u01e8\u0001\u0000\u0000\u0000\u01f0\u01f1"+
		"\u0001\u0000\u0000\u0000\u01f1\u01f2\u0001\u0000\u0000\u0000\u01f2\u01f3"+
		"\u0005\u0003\u0000\u0000\u01f3\u033a\u0001\u0000\u0000\u0000\u01f4\u01f5"+
		"\u0005$\u0000\u0000\u01f5\u01f6\u0005\u00a2\u0000\u0000\u01f6\u01fa\u0003"+
		"\u0098L\u0000\u01f7\u01f8\u0005\u00d8\u0000\u0000\u01f8\u01f9\u0005\u000b"+
		"\u0000\u0000\u01f9\u01fb\u0003\u0092I\u0000\u01fa\u01f7\u0001\u0000\u0000"+
		"\u0000\u01fa\u01fb\u0001\u0000\u0000\u0000\u01fb\u033a\u0001\u0000\u0000"+
		"\u0000\u01fc\u01fd\u00058\u0000\u0000\u01fd\u01fe\u0005\u00a2\u0000\u0000"+
		"\u01fe\u033a\u0003\u0098L\u0000\u01ff\u0200\u0005N\u0000\u0000\u0200\u0201"+
		"\u0003\u0096K\u0000\u0201\u0202\u0005\u00c2\u0000\u0000\u0202\u0207\u0003"+
		"\u0094J\u0000\u0203\u0204\u0005\u0004\u0000\u0000\u0204\u0206\u0003\u0094"+
		"J\u0000\u0205\u0203\u0001\u0000\u0000\u0000\u0206\u0209\u0001\u0000\u0000"+
		"\u0000\u0207\u0205\u0001\u0000\u0000\u0000\u0207\u0208\u0001\u0000\u0000"+
		"\u0000\u0208\u020d\u0001\u0000\u0000\u0000\u0209\u0207\u0001\u0000\u0000"+
		"\u0000\u020a\u020b\u0005\u00d8\u0000\u0000\u020b\u020c\u0005\u000b\u0000"+
		"\u0000\u020c\u020e\u0005\u0086\u0000\u0000\u020d\u020a\u0001\u0000\u0000"+
		"\u0000\u020d\u020e\u0001\u0000\u0000\u0000\u020e\u0212\u0001\u0000\u0000"+
		"\u0000\u020f\u0210\u0005O\u0000\u0000\u0210\u0211\u0005\u0017\u0000\u0000"+
		"\u0211\u0213\u0003\u0092I\u0000\u0212\u020f\u0001\u0000\u0000\u0000\u0212"+
		"\u0213\u0001\u0000\u0000\u0000\u0213\u033a\u0001\u0000\u0000\u0000\u0214"+
		"\u0218\u0005\u00a0\u0000\u0000\u0215\u0216\u0005\u000b\u0000\u0000\u0216"+
		"\u0217\u0005\u0086\u0000\u0000\u0217\u0219\u0005H\u0000\u0000\u0218\u0215"+
		"\u0001\u0000\u0000\u0000\u0218\u0219\u0001\u0000\u0000\u0000\u0219\u021a"+
		"\u0001\u0000\u0000\u0000\u021a\u021b\u0003\u0096K\u0000\u021b\u021c\u0005"+
		"J\u0000\u0000\u021c\u0221\u0003\u0094J\u0000\u021d\u021e\u0005\u0004\u0000"+
		"\u0000\u021e\u0220\u0003\u0094J\u0000\u021f\u021d\u0001\u0000\u0000\u0000"+
		"\u0220\u0223\u0001\u0000\u0000\u0000\u0221\u021f\u0001\u0000\u0000\u0000"+
		"\u0221\u0222\u0001\u0000\u0000\u0000\u0222\u0227\u0001\u0000\u0000\u0000"+
		"\u0223\u0221\u0001\u0000\u0000\u0000\u0224\u0225\u0005O\u0000\u0000\u0225"+
		"\u0226\u0005\u0017\u0000\u0000\u0226\u0228\u0003\u0092I\u0000\u0227\u0224"+
		"\u0001\u0000\u0000\u0000\u0227\u0228\u0001\u0000\u0000\u0000\u0228\u033a"+
		"\u0001\u0000\u0000\u0000\u0229\u022a\u0005\u00af\u0000\u0000\u022a\u022e"+
		"\u0005\u00a2\u0000\u0000\u022b\u022f\u0005\f\u0000\u0000\u022c\u022f\u0005"+
		"|\u0000\u0000\u022d\u022f\u0003\u0098L\u0000\u022e\u022b\u0001\u0000\u0000"+
		"\u0000\u022e\u022c\u0001\u0000\u0000\u0000\u022e\u022d\u0001\u0000\u0000"+
		"\u0000\u022f\u033a\u0001\u0000\u0000\u0000\u0230\u023b\u0005N\u0000\u0000"+
		"\u0231\u0236\u0003\u008cF\u0000\u0232\u0233\u0005\u0004\u0000\u0000\u0233"+
		"\u0235\u0003\u008cF\u0000\u0234\u0232\u0001\u0000\u0000\u0000\u0235\u0238"+
		"\u0001\u0000\u0000\u0000\u0236\u0234\u0001\u0000\u0000\u0000\u0236\u0237"+
		"\u0001\u0000\u0000\u0000\u0237\u023c\u0001\u0000\u0000\u0000\u0238\u0236"+
		"\u0001\u0000\u0000\u0000\u0239\u023a\u0005\f\u0000\u0000\u023a\u023c\u0005"+
		"\u0092\u0000\u0000\u023b\u0231\u0001\u0000\u0000\u0000\u023b\u0239\u0001"+
		"\u0000\u0000\u0000\u023c\u023d\u0001\u0000\u0000\u0000\u023d\u023f\u0005"+
		"\u0084\u0000\u0000\u023e\u0240\u0005\u00ba\u0000\u0000\u023f\u023e\u0001"+
		"\u0000\u0000\u0000\u023f\u0240\u0001\u0000\u0000\u0000\u0240\u0241\u0001"+
		"\u0000\u0000\u0000\u0241\u0242\u0003\u008eG\u0000\u0242\u0243\u0005\u00c2"+
		"\u0000\u0000\u0243\u0247\u0003\u0094J\u0000\u0244\u0245\u0005\u00d8\u0000"+
		"\u0000\u0245\u0246\u0005N\u0000\u0000\u0246\u0248\u0005\u0086\u0000\u0000"+
		"\u0247\u0244\u0001\u0000\u0000\u0000\u0247\u0248\u0001\u0000\u0000\u0000"+
		"\u0248\u033a\u0001\u0000\u0000\u0000\u0249\u024d\u0005\u00a0\u0000\u0000"+
		"\u024a\u024b\u0005N\u0000\u0000\u024b\u024c\u0005\u0086\u0000\u0000\u024c"+
		"\u024e\u0005H\u0000\u0000\u024d\u024a\u0001\u0000\u0000\u0000\u024d\u024e"+
		"\u0001\u0000\u0000\u0000\u024e\u0259\u0001\u0000\u0000\u0000\u024f\u0254"+
		"\u0003\u008cF\u0000\u0250\u0251\u0005\u0004\u0000\u0000\u0251\u0253\u0003"+
		"\u008cF\u0000\u0252\u0250\u0001\u0000\u0000\u0000\u0253\u0256\u0001\u0000"+
		"\u0000\u0000\u0254\u0252\u0001\u0000\u0000\u0000\u0254\u0255\u0001\u0000"+
		"\u0000\u0000\u0255\u025a\u0001\u0000\u0000\u0000\u0256\u0254\u0001\u0000"+
		"\u0000\u0000\u0257\u0258\u0005\f\u0000\u0000\u0258\u025a\u0005\u0092\u0000"+
		"\u0000\u0259\u024f\u0001\u0000\u0000\u0000\u0259\u0257\u0001\u0000\u0000"+
		"\u0000\u025a\u025b\u0001\u0000\u0000\u0000\u025b\u025d\u0005\u0084\u0000"+
		"\u0000\u025c\u025e\u0005\u00ba\u0000\u0000\u025d\u025c\u0001\u0000\u0000"+
		"\u0000\u025d\u025e\u0001\u0000\u0000\u0000\u025e\u025f\u0001\u0000\u0000"+
		"\u0000\u025f\u0260\u0003\u008eG\u0000\u0260\u0261\u0005J\u0000\u0000\u0261"+
		"\u0262\u0003\u0094J\u0000\u0262\u033a\u0001\u0000\u0000\u0000\u0263\u0264"+
		"\u0005\u00b1\u0000\u0000\u0264\u026a\u0005P\u0000\u0000\u0265\u0267\u0005"+
		"\u0084\u0000\u0000\u0266\u0268\u0005\u00ba\u0000\u0000\u0267\u0266\u0001"+
		"\u0000\u0000\u0000\u0267\u0268\u0001\u0000\u0000\u0000\u0268\u0269\u0001"+
		"\u0000\u0000\u0000\u0269\u026b\u0003\u008eG\u0000\u026a\u0265\u0001\u0000"+
		"\u0000\u0000\u026a\u026b\u0001\u0000\u0000\u0000\u026b\u033a\u0001\u0000"+
		"\u0000\u0000\u026c\u026e\u0005@\u0000\u0000\u026d\u026f\u0005\u000e\u0000"+
		"\u0000\u026e\u026d\u0001\u0000\u0000\u0000\u026e\u026f\u0001\u0000\u0000"+
		"\u0000\u026f\u0271\u0001\u0000\u0000\u0000\u0270\u0272\u0005\u00d3\u0000"+
		"\u0000\u0271\u0270\u0001\u0000\u0000\u0000\u0271\u0272\u0001\u0000\u0000"+
		"\u0000\u0272\u027e\u0001\u0000\u0000\u0000\u0273\u0274\u0005\u0002\u0000"+
		"\u0000\u0274\u0279\u0003\u0084B\u0000\u0275\u0276\u0005\u0004\u0000\u0000"+
		"\u0276\u0278\u0003\u0084B\u0000\u0277\u0275\u0001\u0000\u0000\u0000\u0278"+
		"\u027b\u0001\u0000\u0000\u0000\u0279\u0277\u0001\u0000\u0000\u0000\u0279"+
		"\u027a\u0001\u0000\u0000\u0000\u027a\u027c\u0001\u0000\u0000\u0000\u027b"+
		"\u0279\u0001\u0000\u0000\u0000\u027c\u027d\u0005\u0003\u0000\u0000\u027d"+
		"\u027f\u0001\u0000\u0000\u0000\u027e\u0273\u0001\u0000\u0000\u0000\u027e"+
		"\u027f\u0001\u0000\u0000\u0000\u027f\u0280\u0001\u0000\u0000\u0000\u0280"+
		"\u033a\u0003\u0006\u0003\u0000\u0281\u0282\u0005\u00b1\u0000\u0000\u0282"+
		"\u0283\u0005$\u0000\u0000\u0283\u0284\u0005\u00ba\u0000\u0000\u0284\u033a"+
		"\u0003\u008eG\u0000\u0285\u0286\u0005\u00b1\u0000\u0000\u0286\u0287\u0005"+
		"$\u0000\u0000\u0287\u0288\u0005\u00d5\u0000\u0000\u0288\u033a\u0003\u008e"+
		"G\u0000\u0289\u028a\u0005\u00b1\u0000\u0000\u028a\u028b\u0005$\u0000\u0000"+
		"\u028b\u028c\u0005r\u0000\u0000\u028c\u028d\u0005\u00d5\u0000\u0000\u028d"+
		"\u033a\u0003\u008eG\u0000\u028e\u028f\u0005\u00b1\u0000\u0000\u028f\u0290"+
		"\u0005$\u0000\u0000\u0290\u0291\u0005L\u0000\u0000\u0291\u0293\u0003\u008e"+
		"G\u0000\u0292\u0294\u0003p8\u0000\u0293\u0292\u0001\u0000\u0000\u0000"+
		"\u0293\u0294\u0001\u0000\u0000\u0000\u0294\u033a\u0001\u0000\u0000\u0000"+
		"\u0295\u0296\u0005\u00b1\u0000\u0000\u0296\u0299\u0005\u00bb\u0000\u0000"+
		"\u0297\u0298\u0007\u0002\u0000\u0000\u0298\u029a\u0003\u008eG\u0000\u0299"+
		"\u0297\u0001\u0000\u0000\u0000\u0299\u029a\u0001\u0000\u0000\u0000\u029a"+
		"\u02a1\u0001\u0000\u0000\u0000\u029b\u029c\u0005l\u0000\u0000\u029c\u029f"+
		"\u0003^/\u0000\u029d\u029e\u0005;\u0000\u0000\u029e\u02a0\u0003^/\u0000"+
		"\u029f\u029d\u0001\u0000\u0000\u0000\u029f\u02a0\u0001\u0000\u0000\u0000"+
		"\u02a0\u02a2\u0001\u0000\u0000\u0000\u02a1\u029b\u0001\u0000\u0000\u0000"+
		"\u02a1\u02a2\u0001\u0000\u0000\u0000\u02a2\u033a\u0001\u0000\u0000\u0000"+
		"\u02a3\u02a4\u0005\u00b1\u0000\u0000\u02a4\u02a7\u0005\u00a9\u0000\u0000"+
		"\u02a5\u02a6\u0007\u0002\u0000\u0000\u02a6\u02a8\u0003\u0098L\u0000\u02a7"+
		"\u02a5\u0001\u0000\u0000\u0000\u02a7\u02a8\u0001\u0000\u0000\u0000\u02a8"+
		"\u02af\u0001\u0000\u0000\u0000\u02a9\u02aa\u0005l\u0000\u0000\u02aa\u02ad"+
		"\u0003^/\u0000\u02ab\u02ac\u0005;\u0000\u0000\u02ac\u02ae\u0003^/\u0000"+
		"\u02ad\u02ab\u0001\u0000\u0000\u0000\u02ad\u02ae\u0001\u0000\u0000\u0000"+
		"\u02ae\u02b0\u0001\u0000\u0000\u0000\u02af\u02a9\u0001\u0000\u0000\u0000"+
		"\u02af\u02b0\u0001\u0000\u0000\u0000\u02b0\u033a\u0001\u0000\u0000\u0000"+
		"\u02b1\u02b2\u0005\u00b1\u0000\u0000\u02b2\u02b9\u0005\u001d\u0000\u0000"+
		"\u02b3\u02b4\u0005l\u0000\u0000\u02b4\u02b7\u0003^/\u0000\u02b5\u02b6"+
		"\u0005;\u0000\u0000\u02b6\u02b8\u0003^/\u0000\u02b7\u02b5\u0001\u0000"+
		"\u0000\u0000\u02b7\u02b8\u0001\u0000\u0000\u0000\u02b8\u02ba\u0001\u0000"+
		"\u0000\u0000\u02b9\u02b3\u0001\u0000\u0000\u0000\u02b9\u02ba\u0001\u0000"+
		"\u0000\u0000\u02ba\u033a\u0001\u0000\u0000\u0000\u02bb\u02bc\u0005\u00b1"+
		"\u0000\u0000\u02bc\u02bd\u0005\u001f\u0000\u0000\u02bd\u02be\u0007\u0002"+
		"\u0000\u0000\u02be\u033a\u0003\u008eG\u0000\u02bf\u02c0\u0005\u00b1\u0000"+
		"\u0000\u02c0\u02c1\u0005\u00b5\u0000\u0000\u02c1\u02c2\u0005H\u0000\u0000"+
		"\u02c2\u033a\u0003\u008eG\u0000\u02c3\u02c4\u0005\u00b1\u0000\u0000\u02c4"+
		"\u02c5\u0005\u00b5\u0000\u0000\u02c5\u02c6\u0005H\u0000\u0000\u02c6\u02c7"+
		"\u0005\u0002\u0000\u0000\u02c7\u02c8\u00036\u001b\u0000\u02c8\u02c9\u0005"+
		"\u0003\u0000\u0000\u02c9\u033a\u0001\u0000\u0000\u0000\u02ca\u02cc\u0005"+
		"\u00b1\u0000\u0000\u02cb\u02cd\u0005\'\u0000\u0000\u02cc\u02cb\u0001\u0000"+
		"\u0000\u0000\u02cc\u02cd\u0001\u0000\u0000\u0000\u02cd\u02ce\u0001\u0000"+
		"\u0000\u0000\u02ce\u02d1\u0005\u00a3\u0000\u0000\u02cf\u02d0\u0007\u0002"+
		"\u0000\u0000\u02d0\u02d2\u0003\u0098L\u0000\u02d1\u02cf\u0001\u0000\u0000"+
		"\u0000\u02d1\u02d2\u0001\u0000\u0000\u0000\u02d2\u033a\u0001\u0000\u0000"+
		"\u0000\u02d3\u02d4\u0005\u00b1\u0000\u0000\u02d4\u02d5\u0005\u00a2\u0000"+
		"\u0000\u02d5\u02d8\u0005P\u0000\u0000\u02d6\u02d7\u0007\u0002\u0000\u0000"+
		"\u02d7\u02d9\u0003\u0098L\u0000\u02d8\u02d6\u0001\u0000\u0000\u0000\u02d8"+
		"\u02d9\u0001\u0000\u0000\u0000\u02d9\u033a\u0001\u0000\u0000\u0000\u02da"+
		"\u02db\u00054\u0000\u0000\u02db\u033a\u0003\u008eG\u0000\u02dc\u02dd\u0005"+
		"3\u0000\u0000\u02dd\u033a\u0003\u008eG\u0000\u02de\u02df\u0005\u00b1\u0000"+
		"\u0000\u02df\u02e6\u0005M\u0000\u0000\u02e0\u02e1\u0005l\u0000\u0000\u02e1"+
		"\u02e4\u0003^/\u0000\u02e2\u02e3\u0005;\u0000\u0000\u02e3\u02e5\u0003"+
		"^/\u0000\u02e4\u02e2\u0001\u0000\u0000\u0000\u02e4\u02e5\u0001\u0000\u0000"+
		"\u0000\u02e5\u02e7\u0001\u0000\u0000\u0000\u02e6\u02e0\u0001\u0000\u0000"+
		"\u0000\u02e6\u02e7\u0001\u0000\u0000\u0000\u02e7\u033a\u0001\u0000\u0000"+
		"\u0000\u02e8\u02e9\u0005\u00b1\u0000\u0000\u02e9\u02f0\u0005\u00ae\u0000"+
		"\u0000\u02ea\u02eb\u0005l\u0000\u0000\u02eb\u02ee\u0003^/\u0000\u02ec"+
		"\u02ed\u0005;\u0000\u0000\u02ed\u02ef\u0003^/\u0000\u02ee\u02ec\u0001"+
		"\u0000\u0000\u0000\u02ee\u02ef\u0001\u0000\u0000\u0000\u02ef\u02f1\u0001"+
		"\u0000\u0000\u0000\u02f0\u02ea\u0001\u0000\u0000\u0000\u02f0\u02f1\u0001"+
		"\u0000\u0000\u0000\u02f1\u033a\u0001\u0000\u0000\u0000\u02f2\u02f3\u0005"+
		"\u00af\u0000\u0000\u02f3\u02f4\u0005\u00ae\u0000\u0000\u02f4\u02f5\u0003"+
		"\u008eG\u0000\u02f5\u02f6\u0005\u00dd\u0000\u0000\u02f6\u02f7\u0003T*"+
		"\u0000\u02f7\u033a\u0001\u0000\u0000\u0000\u02f8\u02f9\u0005\u009b\u0000"+
		"\u0000\u02f9\u02fa\u0005\u00ae\u0000\u0000\u02fa\u033a\u0003\u008eG\u0000"+
		"\u02fb\u02fc\u0005\u00b4\u0000\u0000\u02fc\u0305\u0005\u00c3\u0000\u0000"+
		"\u02fd\u0302\u0003\u0086C\u0000\u02fe\u02ff\u0005\u0004\u0000\u0000\u02ff"+
		"\u0301\u0003\u0086C\u0000\u0300\u02fe\u0001\u0000\u0000\u0000\u0301\u0304"+
		"\u0001\u0000\u0000\u0000\u0302\u0300\u0001\u0000\u0000\u0000\u0302\u0303"+
		"\u0001\u0000\u0000\u0000\u0303\u0306\u0001\u0000\u0000\u0000\u0304\u0302"+
		"\u0001\u0000\u0000\u0000\u0305\u02fd\u0001\u0000\u0000\u0000\u0305\u0306"+
		"\u0001\u0000\u0000\u0000\u0306\u033a\u0001\u0000\u0000\u0000\u0307\u0309"+
		"\u0005!\u0000\u0000\u0308\u030a\u0005\u00d9\u0000\u0000\u0309\u0308\u0001"+
		"\u0000\u0000\u0000\u0309\u030a\u0001\u0000\u0000\u0000\u030a\u033a\u0001"+
		"\u0000\u0000\u0000\u030b\u030d\u0005\u00a4\u0000\u0000\u030c\u030e\u0005"+
		"\u00d9\u0000\u0000\u030d\u030c\u0001\u0000\u0000\u0000\u030d\u030e\u0001"+
		"\u0000\u0000\u0000\u030e\u033a\u0001\u0000\u0000\u0000\u030f\u0310\u0005"+
		"\u0091\u0000\u0000\u0310\u0311\u0003\u0098L\u0000\u0311\u0312\u0005J\u0000"+
		"\u0000\u0312\u0313\u0003\u0006\u0003\u0000\u0313\u033a\u0001\u0000\u0000"+
		"\u0000\u0314\u0315\u00050\u0000\u0000\u0315\u0316\u0005\u0091\u0000\u0000"+
		"\u0316\u033a\u0003\u0098L\u0000\u0317\u0318\u0005>\u0000\u0000\u0318\u0322"+
		"\u0003\u0098L\u0000\u0319\u031a\u0005\u00d0\u0000\u0000\u031a\u031f\u0003"+
		"T*\u0000\u031b\u031c\u0005\u0004\u0000\u0000\u031c\u031e\u0003T*\u0000"+
		"\u031d\u031b\u0001\u0000\u0000\u0000\u031e\u0321\u0001\u0000\u0000\u0000"+
		"\u031f\u031d\u0001\u0000\u0000\u0000\u031f\u0320\u0001\u0000\u0000\u0000"+
		"\u0320\u0323\u0001\u0000\u0000\u0000\u0321\u031f\u0001\u0000\u0000\u0000"+
		"\u0322\u0319\u0001\u0000\u0000\u0000\u0322\u0323\u0001\u0000\u0000\u0000"+
		"\u0323\u033a\u0001\u0000\u0000\u0000\u0324\u0325\u00054\u0000\u0000\u0325"+
		"\u0326\u0005\\\u0000\u0000\u0326\u033a\u0003\u0098L\u0000\u0327\u0328"+
		"\u00054\u0000\u0000\u0328\u0329\u0005\u008b\u0000\u0000\u0329\u033a\u0003"+
		"\u0098L\u0000\u032a\u032b\u0005\u00cd\u0000\u0000\u032b\u032c\u0003\u008e"+
		"G\u0000\u032c\u032d\u0005\u00af\u0000\u0000\u032d\u0332\u0003\u0082A\u0000"+
		"\u032e\u032f\u0005\u0004\u0000\u0000\u032f\u0331\u0003\u0082A\u0000\u0330"+
		"\u032e\u0001\u0000\u0000\u0000\u0331\u0334\u0001\u0000\u0000\u0000\u0332"+
		"\u0330\u0001\u0000\u0000\u0000\u0332\u0333\u0001\u0000\u0000\u0000\u0333"+
		"\u0337\u0001\u0000\u0000\u0000\u0334\u0332\u0001\u0000\u0000\u0000\u0335"+
		"\u0336\u0005\u00d7\u0000\u0000\u0336\u0338\u0003V+\u0000\u0337\u0335\u0001"+
		"\u0000\u0000\u0000\u0337\u0338\u0001\u0000\u0000\u0000\u0338\u033a\u0001"+
		"\u0000\u0000\u0000\u0339\u00a7\u0001\u0000\u0000\u0000\u0339\u00a8\u0001"+
		"\u0000\u0000\u0000\u0339\u00aa\u0001\u0000\u0000\u0000\u0339\u00af\u0001"+
		"\u0000\u0000\u0000\u0339\u00bb\u0001\u0000\u0000\u0000\u0339\u00c5\u0001"+
		"\u0000\u0000\u0000\u0339\u00cc\u0001\u0000\u0000\u0000\u0339\u00ee\u0001"+
		"\u0000\u0000\u0000\u0339\u0108\u0001\u0000\u0000\u0000\u0339\u010f\u0001"+
		"\u0000\u0000\u0000\u0339\u0117\u0001\u0000\u0000\u0000\u0339\u011e\u0001"+
		"\u0000\u0000\u0000\u0339\u0121\u0001\u0000\u0000\u0000\u0339\u012c\u0001"+
		"\u0000\u0000\u0000\u0339\u013d\u0001\u0000\u0000\u0000\u0339\u014c\u0001"+
		"\u0000\u0000\u0000\u0339\u015c\u0001\u0000\u0000\u0000\u0339\u0162\u0001"+
		"\u0000\u0000\u0000\u0339\u0174\u0001\u0000\u0000\u0000\u0339\u0182\u0001"+
		"\u0000\u0000\u0000\u0339\u0189\u0001\u0000\u0000\u0000\u0339\u01a2\u0001"+
		"\u0000\u0000\u0000\u0339\u01aa\u0001\u0000\u0000\u0000\u0339\u01b1\u0001"+
		"\u0000\u0000\u0000\u0339\u01d0\u0001\u0000\u0000\u0000\u0339\u01d8\u0001"+
		"\u0000\u0000\u0000\u0339\u01e5\u0001\u0000\u0000\u0000\u0339\u01f4\u0001"+
		"\u0000\u0000\u0000\u0339\u01fc\u0001\u0000\u0000\u0000\u0339\u01ff\u0001"+
		"\u0000\u0000\u0000\u0339\u0214\u0001\u0000\u0000\u0000\u0339\u0229\u0001"+
		"\u0000\u0000\u0000\u0339\u0230\u0001\u0000\u0000\u0000\u0339\u0249\u0001"+
		"\u0000\u0000\u0000\u0339\u0263\u0001\u0000\u0000\u0000\u0339\u026c\u0001"+
		"\u0000\u0000\u0000\u0339\u0281\u0001\u0000\u0000\u0000\u0339\u0285\u0001"+
		"\u0000\u0000\u0000\u0339\u0289\u0001\u0000\u0000\u0000\u0339\u028e\u0001"+
		"\u0000\u0000\u0000\u0339\u0295\u0001\u0000\u0000\u0000\u0339\u02a3\u0001"+
		"\u0000\u0000\u0000\u0339\u02b1\u0001\u0000\u0000\u0000\u0339\u02bb\u0001"+
		"\u0000\u0000\u0000\u0339\u02bf\u0001\u0000\u0000\u0000\u0339\u02c3\u0001"+
		"\u0000\u0000\u0000\u0339\u02ca\u0001\u0000\u0000\u0000\u0339\u02d3\u0001"+
		"\u0000\u0000\u0000\u0339\u02da\u0001\u0000\u0000\u0000\u0339\u02dc\u0001"+
		"\u0000\u0000\u0000\u0339\u02de\u0001\u0000\u0000\u0000\u0339\u02e8\u0001"+
		"\u0000\u0000\u0000\u0339\u02f2\u0001\u0000\u0000\u0000\u0339\u02f8\u0001"+
		"\u0000\u0000\u0000\u0339\u02fb\u0001\u0000\u0000\u0000\u0339\u0307\u0001"+
		"\u0000\u0000\u0000\u0339\u030b\u0001\u0000\u0000\u0000\u0339\u030f\u0001"+
		"\u0000\u0000\u0000\u0339\u0314\u0001\u0000\u0000\u0000\u0339\u0317\u0001"+
		"\u0000\u0000\u0000\u0339\u0324\u0001\u0000\u0000\u0000\u0339\u0327\u0001"+
		"\u0000\u0000\u0000\u0339\u032a\u0001\u0000\u0000\u0000\u033a\u0007\u0001"+
		"\u0000\u0000\u0000\u033b\u033d\u0003\n\u0005\u0000\u033c\u033b\u0001\u0000"+
		"\u0000\u0000\u033c\u033d\u0001\u0000\u0000\u0000\u033d\u033e\u0001\u0000"+
		"\u0000\u0000\u033e\u033f\u0003.\u0017\u0000\u033f\t\u0001\u0000\u0000"+
		"\u0000\u0340\u0342\u0005\u00d8\u0000\u0000\u0341\u0343\u0005\u0096\u0000"+
		"\u0000\u0342\u0341\u0001\u0000\u0000\u0000\u0342\u0343\u0001\u0000\u0000"+
		"\u0000\u0343\u0344\u0001\u0000\u0000\u0000\u0344\u0349\u0003>\u001f\u0000"+
		"\u0345\u0346\u0005\u0004\u0000\u0000\u0346\u0348\u0003>\u001f\u0000\u0347"+
		"\u0345\u0001\u0000\u0000\u0000\u0348\u034b\u0001\u0000\u0000\u0000\u0349"+
		"\u0347\u0001\u0000\u0000\u0000\u0349\u034a\u0001\u0000\u0000\u0000\u034a"+
		"\u000b\u0001\u0000\u0000\u0000\u034b\u0349\u0001\u0000\u0000\u0000\u034c"+
		"\u034f\u0003\u000e\u0007\u0000\u034d\u034f\u0003\u0010\b\u0000\u034e\u034c"+
		"\u0001\u0000\u0000\u0000\u034e\u034d\u0001\u0000\u0000\u0000\u034f\r\u0001"+
		"\u0000\u0000\u0000\u0350\u0351\u0003\u0098L\u0000\u0351\u0354\u0003r9"+
		"\u0000\u0352\u0353\u0005~\u0000\u0000\u0353\u0355\u0005\u007f\u0000\u0000"+
		"\u0354\u0352\u0001\u0000\u0000\u0000\u0354\u0355\u0001\u0000\u0000\u0000"+
		"\u0355\u0358\u0001\u0000\u0000\u0000\u0356\u0357\u0005 \u0000\u0000\u0357"+
		"\u0359\u0003^/\u0000\u0358\u0356\u0001\u0000\u0000\u0000\u0358\u0359\u0001"+
		"\u0000\u0000\u0000\u0359\u035c\u0001\u0000\u0000\u0000\u035a\u035b\u0005"+
		"\u00d8\u0000\u0000\u035b\u035d\u0003\u0012\t\u0000\u035c\u035a\u0001\u0000"+
		"\u0000\u0000\u035c\u035d\u0001\u0000\u0000\u0000\u035d\u000f\u0001\u0000"+
		"\u0000\u0000\u035e\u035f\u0005l\u0000\u0000\u035f\u0362\u0003\u008eG\u0000"+
		"\u0360\u0361\u0007\u0003\u0000\u0000\u0361\u0363\u0005\u0093\u0000\u0000"+
		"\u0362\u0360\u0001\u0000\u0000\u0000\u0362\u0363\u0001\u0000\u0000\u0000"+
		"\u0363\u0011\u0001\u0000\u0000\u0000\u0364\u0365\u0005\u0002\u0000\u0000"+
		"\u0365\u036a\u0003\u0014\n\u0000\u0366\u0367\u0005\u0004\u0000\u0000\u0367"+
		"\u0369\u0003\u0014\n\u0000\u0368\u0366\u0001\u0000\u0000\u0000\u0369\u036c"+
		"\u0001\u0000\u0000\u0000\u036a\u0368\u0001\u0000\u0000\u0000\u036a\u036b"+
		"\u0001\u0000\u0000\u0000\u036b\u036d\u0001\u0000\u0000\u0000\u036c\u036a"+
		"\u0001\u0000\u0000\u0000\u036d\u036e\u0005\u0003\u0000\u0000\u036e\u0013"+
		"\u0001\u0000\u0000\u0000\u036f\u0370\u0003\u0098L\u0000\u0370\u0371\u0005"+
		"\u00dd\u0000\u0000\u0371\u0372\u0003T*\u0000\u0372\u0015\u0001\u0000\u0000"+
		"\u0000\u0373\u0374\u0003\u0098L\u0000\u0374\u0375\u0003r9\u0000\u0375"+
		"\u0017\u0001\u0000\u0000\u0000\u0376\u0378\u0003\u001a\r\u0000\u0377\u0376"+
		"\u0001\u0000\u0000\u0000\u0378\u037b\u0001\u0000\u0000\u0000\u0379\u0377"+
		"\u0001\u0000\u0000\u0000\u0379\u037a\u0001\u0000\u0000\u0000\u037a\u0019"+
		"\u0001\u0000\u0000\u0000\u037b\u0379\u0001\u0000\u0000\u0000\u037c\u037d"+
		"\u0005g\u0000\u0000\u037d\u0381\u0003&\u0013\u0000\u037e\u0381\u0003("+
		"\u0014\u0000\u037f\u0381\u0003*\u0015\u0000\u0380\u037c\u0001\u0000\u0000"+
		"\u0000\u0380\u037e\u0001\u0000\u0000\u0000\u0380\u037f\u0001\u0000\u0000"+
		"\u0000\u0381\u001b\u0001\u0000\u0000\u0000\u0382\u0384\u0003\u001e\u000f"+
		"\u0000\u0383\u0382\u0001\u0000\u0000\u0000\u0384\u0387\u0001\u0000\u0000"+
		"\u0000\u0385\u0383\u0001\u0000\u0000\u0000\u0385\u0386\u0001\u0000\u0000"+
		"\u0000\u0386\u001d\u0001\u0000\u0000\u0000\u0387\u0385\u0001\u0000\u0000"+
		"\u0000\u0388\u0389\u0003*\u0015\u0000\u0389\u001f\u0001\u0000\u0000\u0000"+
		"\u038a\u038d\u0003\"\u0011\u0000\u038b\u038d\u0003$\u0012\u0000\u038c"+
		"\u038a\u0001\u0000\u0000\u0000\u038c\u038b\u0001\u0000\u0000\u0000\u038d"+
		"!\u0001\u0000\u0000\u0000\u038e\u038f\u0005\u009e\u0000\u0000\u038f\u0390"+
		"\u0003T*\u0000\u0390#\u0001\u0000\u0000\u0000\u0391\u0394\u0005B\u0000"+
		"\u0000\u0392\u0393\u0005u\u0000\u0000\u0393\u0395\u0003,\u0016\u0000\u0394"+
		"\u0392\u0001\u0000\u0000\u0000\u0394\u0395\u0001\u0000\u0000\u0000\u0395"+
		"%\u0001\u0000\u0000\u0000\u0396\u0399\u0005\u00b3\u0000\u0000\u0397\u0399"+
		"\u0003\u0098L\u0000\u0398\u0396\u0001\u0000\u0000\u0000\u0398\u0397\u0001"+
		"\u0000\u0000\u0000\u0399\'\u0001\u0000\u0000\u0000\u039a\u039e\u00055"+
		"\u0000\u0000\u039b\u039c\u0005~\u0000\u0000\u039c\u039e\u00055\u0000\u0000"+
		"\u039d\u039a\u0001\u0000\u0000\u0000\u039d\u039b\u0001\u0000\u0000\u0000"+
		"\u039e)\u0001\u0000\u0000\u0000\u039f\u03a0\u0005\u009f\u0000\u0000\u03a0"+
		"\u03a1\u0005\u007f\u0000\u0000\u03a1\u03a2\u0005\u0084\u0000\u0000\u03a2"+
		"\u03a3\u0005\u007f\u0000\u0000\u03a3\u03a9\u0005\\\u0000\u0000\u03a4\u03a5"+
		"\u0005\u0019\u0000\u0000\u03a5\u03a6\u0005\u0084\u0000\u0000\u03a6\u03a7"+
		"\u0005\u007f\u0000\u0000\u03a7\u03a9\u0005\\\u0000\u0000\u03a8\u039f\u0001"+
		"\u0000\u0000\u0000\u03a8\u03a4\u0001\u0000\u0000\u0000\u03a9+\u0001\u0000"+
		"\u0000\u0000\u03aa\u03ab\u0003\u0098L\u0000\u03ab-\u0001\u0000\u0000\u0000"+
		"\u03ac\u03b7\u00030\u0018\u0000\u03ad\u03ae\u0005\u0088\u0000\u0000\u03ae"+
		"\u03af\u0005\u0017\u0000\u0000\u03af\u03b4\u00034\u001a\u0000\u03b0\u03b1"+
		"\u0005\u0004\u0000\u0000\u03b1\u03b3\u00034\u001a\u0000\u03b2\u03b0\u0001"+
		"\u0000\u0000\u0000\u03b3\u03b6\u0001\u0000\u0000\u0000\u03b4\u03b2\u0001"+
		"\u0000\u0000\u0000\u03b4\u03b5\u0001\u0000\u0000\u0000\u03b5\u03b8\u0001"+
		"\u0000\u0000\u0000\u03b6\u03b4\u0001\u0000\u0000\u0000\u03b7\u03ad\u0001"+
		"\u0000\u0000\u0000\u03b7\u03b8\u0001\u0000\u0000\u0000\u03b8\u03be\u0001"+
		"\u0000\u0000\u0000\u03b9\u03ba\u0005\u0083\u0000\u0000\u03ba\u03bc\u0005"+
		"\u00ec\u0000\u0000\u03bb\u03bd\u0007\u0004\u0000\u0000\u03bc\u03bb\u0001"+
		"\u0000\u0000\u0000\u03bc\u03bd\u0001\u0000\u0000\u0000\u03bd\u03bf\u0001"+
		"\u0000\u0000\u0000\u03be\u03b9\u0001\u0000\u0000\u0000\u03be\u03bf\u0001"+
		"\u0000\u0000\u0000\u03bf\u03c9\u0001\u0000\u0000\u0000\u03c0\u03c1\u0005"+
		"m\u0000\u0000\u03c1\u03c8\u0007\u0005\u0000\u0000\u03c2\u03c3\u0005D\u0000"+
		"\u0000\u03c3\u03c4\u0005F\u0000\u0000\u03c4\u03c5\u0005\u00ec\u0000\u0000"+
		"\u03c5\u03c6\u0005\u00a7\u0000\u0000\u03c6\u03c8\u0005\u0085\u0000\u0000"+
		"\u03c7\u03c0\u0001\u0000\u0000\u0000\u03c7\u03c2\u0001\u0000\u0000\u0000"+
		"\u03c7\u03c8\u0001\u0000\u0000\u0000\u03c8\u03ca\u0001\u0000\u0000\u0000"+
		"\u03c9\u03c7\u0001\u0000\u0000\u0000\u03c9\u03ca\u0001\u0000\u0000\u0000"+
		"\u03ca/\u0001\u0000\u0000\u0000\u03cb\u03cc\u0006\u0018\uffff\uffff\u0000"+
		"\u03cc\u03cd\u00032\u0019\u0000\u03cd\u03dc\u0001\u0000\u0000\u0000\u03ce"+
		"\u03cf\n\u0002\u0000\u0000\u03cf\u03d1\u0005^\u0000\u0000\u03d0\u03d2"+
		"\u0003@ \u0000\u03d1\u03d0\u0001\u0000\u0000\u0000\u03d1\u03d2\u0001\u0000"+
		"\u0000\u0000\u03d2\u03d3\u0001\u0000\u0000\u0000\u03d3\u03db\u00030\u0018"+
		"\u0003\u03d4\u03d5\n\u0001\u0000\u0000\u03d5\u03d7\u0007\u0006\u0000\u0000"+
		"\u03d6\u03d8\u0003@ \u0000\u03d7\u03d6\u0001\u0000\u0000\u0000\u03d7\u03d8"+
		"\u0001\u0000\u0000\u0000\u03d8\u03d9\u0001\u0000\u0000\u0000\u03d9\u03db"+
		"\u00030\u0018\u0002\u03da\u03ce\u0001\u0000\u0000\u0000\u03da\u03d4\u0001"+
		"\u0000\u0000\u0000\u03db\u03de\u0001\u0000\u0000\u0000\u03dc\u03da\u0001"+
		"\u0000\u0000\u0000\u03dc\u03dd\u0001\u0000\u0000\u0000\u03dd1\u0001\u0000"+
		"\u0000\u0000\u03de\u03dc\u0001\u0000\u0000\u0000\u03df\u03f0\u00036\u001b"+
		"\u0000\u03e0\u03e1\u0005\u00ba\u0000\u0000\u03e1\u03f0\u0003\u008eG\u0000"+
		"\u03e2\u03e3\u0005\u00d2\u0000\u0000\u03e3\u03e8\u0003T*\u0000\u03e4\u03e5"+
		"\u0005\u0004\u0000\u0000\u03e5\u03e7\u0003T*\u0000\u03e6\u03e4\u0001\u0000"+
		"\u0000\u0000\u03e7\u03ea\u0001\u0000\u0000\u0000\u03e8\u03e6\u0001\u0000"+
		"\u0000\u0000\u03e8\u03e9\u0001\u0000\u0000\u0000\u03e9\u03f0\u0001\u0000"+
		"\u0000\u0000\u03ea\u03e8\u0001\u0000\u0000\u0000\u03eb\u03ec\u0005\u0002"+
		"\u0000\u0000\u03ec\u03ed\u0003.\u0017\u0000\u03ed\u03ee\u0005\u0003\u0000"+
		"\u0000\u03ee\u03f0\u0001\u0000\u0000\u0000\u03ef\u03df\u0001\u0000\u0000"+
		"\u0000\u03ef\u03e0\u0001\u0000\u0000\u0000\u03ef\u03e2\u0001\u0000\u0000"+
		"\u0000\u03ef\u03eb\u0001\u0000\u0000\u0000\u03f03\u0001\u0000\u0000\u0000"+
		"\u03f1\u03f3\u0003T*\u0000\u03f2\u03f4\u0007\u0007\u0000\u0000\u03f3\u03f2"+
		"\u0001\u0000\u0000\u0000\u03f3\u03f4\u0001\u0000\u0000\u0000\u03f4\u03f7"+
		"\u0001\u0000\u0000\u0000\u03f5\u03f6\u0005\u0081\u0000\u0000\u03f6\u03f8"+
		"\u0007\b\u0000\u0000\u03f7\u03f5\u0001\u0000\u0000\u0000\u03f7\u03f8\u0001"+
		"\u0000\u0000\u0000\u03f85\u0001\u0000\u0000\u0000\u03f9\u03fb\u0005\u00ac"+
		"\u0000\u0000\u03fa\u03fc\u0003@ \u0000\u03fb\u03fa\u0001\u0000\u0000\u0000"+
		"\u03fb\u03fc\u0001\u0000\u0000\u0000\u03fc\u03fd\u0001\u0000\u0000\u0000"+
		"\u03fd\u0402\u0003B!\u0000\u03fe\u03ff\u0005\u0004\u0000\u0000\u03ff\u0401"+
		"\u0003B!\u0000\u0400\u03fe\u0001\u0000\u0000\u0000\u0401\u0404\u0001\u0000"+
		"\u0000\u0000\u0402\u0400\u0001\u0000\u0000\u0000\u0402\u0403\u0001\u0000"+
		"\u0000\u0000\u0403\u040e\u0001\u0000\u0000\u0000\u0404\u0402\u0001\u0000"+
		"\u0000\u0000\u0405\u0406\u0005J\u0000\u0000\u0406\u040b\u0003D\"\u0000"+
		"\u0407\u0408\u0005\u0004\u0000\u0000\u0408\u040a\u0003D\"\u0000\u0409"+
		"\u0407\u0001\u0000\u0000\u0000\u040a\u040d\u0001\u0000\u0000\u0000\u040b"+
		"\u0409\u0001\u0000\u0000\u0000\u040b\u040c\u0001\u0000\u0000\u0000\u040c"+
		"\u040f\u0001\u0000\u0000\u0000\u040d\u040b\u0001\u0000\u0000\u0000\u040e"+
		"\u0405\u0001\u0000\u0000\u0000\u040e\u040f\u0001\u0000\u0000\u0000\u040f"+
		"\u0412\u0001\u0000\u0000\u0000\u0410\u0411\u0005\u00d7\u0000\u0000\u0411"+
		"\u0413\u0003V+\u0000\u0412\u0410\u0001\u0000\u0000\u0000\u0412\u0413\u0001"+
		"\u0000\u0000\u0000\u0413\u0417\u0001\u0000\u0000\u0000\u0414\u0415\u0005"+
		"R\u0000\u0000\u0415\u0416\u0005\u0017\u0000\u0000\u0416\u0418\u00038\u001c"+
		"\u0000\u0417\u0414\u0001\u0000\u0000\u0000\u0417\u0418\u0001\u0000\u0000"+
		"\u0000\u0418\u041b\u0001\u0000\u0000\u0000\u0419\u041a\u0005U\u0000\u0000"+
		"\u041a\u041c\u0003V+\u0000\u041b\u0419\u0001\u0000\u0000\u0000\u041b\u041c"+
		"\u0001\u0000\u0000\u0000\u041c7\u0001\u0000\u0000\u0000\u041d\u041f\u0003"+
		"@ \u0000\u041e\u041d\u0001\u0000\u0000\u0000\u041e\u041f\u0001\u0000\u0000"+
		"\u0000\u041f\u0420\u0001\u0000\u0000\u0000\u0420\u0425\u0003:\u001d\u0000"+
		"\u0421\u0422\u0005\u0004\u0000\u0000\u0422\u0424\u0003:\u001d\u0000\u0423"+
		"\u0421\u0001\u0000\u0000\u0000\u0424\u0427\u0001\u0000\u0000\u0000\u0425"+
		"\u0423\u0001\u0000\u0000\u0000\u0425\u0426\u0001\u0000\u0000\u0000\u0426"+
		"9\u0001\u0000\u0000\u0000\u0427\u0425\u0001\u0000\u0000\u0000\u0428\u0451"+
		"\u0003<\u001e\u0000\u0429\u042a\u0005\u00a5\u0000\u0000\u042a\u0433\u0005"+
		"\u0002\u0000\u0000\u042b\u0430\u0003T*\u0000\u042c\u042d\u0005\u0004\u0000"+
		"\u0000\u042d\u042f\u0003T*\u0000\u042e\u042c\u0001\u0000\u0000\u0000\u042f"+
		"\u0432\u0001\u0000\u0000\u0000\u0430\u042e\u0001\u0000\u0000\u0000\u0430"+
		"\u0431\u0001\u0000\u0000\u0000\u0431\u0434\u0001\u0000\u0000\u0000\u0432"+
		"\u0430\u0001\u0000\u0000\u0000\u0433\u042b\u0001\u0000\u0000\u0000\u0433"+
		"\u0434\u0001\u0000\u0000\u0000\u0434\u0435\u0001\u0000\u0000\u0000\u0435"+
		"\u0451\u0005\u0003\u0000\u0000\u0436\u0437\u0005&\u0000\u0000\u0437\u0440"+
		"\u0005\u0002\u0000\u0000\u0438\u043d\u0003T*\u0000\u0439\u043a\u0005\u0004"+
		"\u0000\u0000\u043a\u043c\u0003T*\u0000\u043b\u0439\u0001\u0000\u0000\u0000"+
		"\u043c\u043f\u0001\u0000\u0000\u0000\u043d\u043b\u0001\u0000\u0000\u0000"+
		"\u043d\u043e\u0001\u0000\u0000\u0000\u043e\u0441\u0001\u0000\u0000\u0000"+
		"\u043f\u043d\u0001\u0000\u0000\u0000\u0440\u0438\u0001\u0000\u0000\u0000"+
		"\u0440\u0441\u0001\u0000\u0000\u0000\u0441\u0442\u0001\u0000\u0000\u0000"+
		"\u0442\u0451\u0005\u0003\u0000\u0000\u0443\u0444\u0005S\u0000\u0000\u0444"+
		"\u0445\u0005\u00b0\u0000\u0000\u0445\u0446\u0005\u0002\u0000\u0000\u0446"+
		"\u044b\u0003<\u001e\u0000\u0447\u0448\u0005\u0004\u0000\u0000\u0448\u044a"+
		"\u0003<\u001e\u0000\u0449\u0447\u0001\u0000\u0000\u0000\u044a\u044d\u0001"+
		"\u0000\u0000\u0000\u044b\u0449\u0001\u0000\u0000\u0000\u044b\u044c\u0001"+
		"\u0000\u0000\u0000\u044c\u044e\u0001\u0000\u0000\u0000\u044d\u044b\u0001"+
		"\u0000\u0000\u0000\u044e\u044f\u0005\u0003\u0000\u0000\u044f\u0451\u0001"+
		"\u0000\u0000\u0000\u0450\u0428\u0001\u0000\u0000\u0000\u0450\u0429\u0001"+
		"\u0000\u0000\u0000\u0450\u0436\u0001\u0000\u0000\u0000\u0450\u0443\u0001"+
		"\u0000\u0000\u0000\u0451;\u0001\u0000\u0000\u0000\u0452\u045b\u0005\u0002"+
		"\u0000\u0000\u0453\u0458\u0003T*\u0000\u0454\u0455\u0005\u0004\u0000\u0000"+
		"\u0455\u0457\u0003T*\u0000\u0456\u0454\u0001\u0000\u0000\u0000\u0457\u045a"+
		"\u0001\u0000\u0000\u0000\u0458\u0456\u0001\u0000\u0000\u0000\u0458\u0459"+
		"\u0001\u0000\u0000\u0000\u0459\u045c\u0001\u0000\u0000\u0000\u045a\u0458"+
		"\u0001\u0000\u0000\u0000\u045b\u0453\u0001\u0000\u0000\u0000\u045b\u045c"+
		"\u0001\u0000\u0000\u0000\u045c\u045d\u0001\u0000\u0000\u0000\u045d\u0460"+
		"\u0005\u0003\u0000\u0000\u045e\u0460\u0003T*\u0000\u045f\u0452\u0001\u0000"+
		"\u0000\u0000\u045f\u045e\u0001\u0000\u0000\u0000\u0460=\u0001\u0000\u0000"+
		"\u0000\u0461\u0463\u0003\u0098L\u0000\u0462\u0464\u0003P(\u0000\u0463"+
		"\u0462\u0001\u0000\u0000\u0000\u0463\u0464\u0001\u0000\u0000\u0000\u0464"+
		"\u0465\u0001\u0000\u0000\u0000\u0465\u0466\u0005\u0012\u0000\u0000\u0466"+
		"\u0467\u0005\u0002\u0000\u0000\u0467\u0468\u0003\b\u0004\u0000\u0468\u0469"+
		"\u0005\u0003\u0000\u0000\u0469?\u0001\u0000\u0000\u0000\u046a\u046b\u0007"+
		"\t\u0000\u0000\u046bA\u0001\u0000\u0000\u0000\u046c\u0471\u0003T*\u0000"+
		"\u046d\u046f\u0005\u0012\u0000\u0000\u046e\u046d\u0001\u0000\u0000\u0000"+
		"\u046e\u046f\u0001\u0000\u0000\u0000\u046f\u0470\u0001\u0000\u0000\u0000"+
		"\u0470\u0472\u0003\u0098L\u0000\u0471\u046e\u0001\u0000\u0000\u0000\u0471"+
		"\u0472\u0001\u0000\u0000\u0000\u0472\u0479\u0001\u0000\u0000\u0000\u0473"+
		"\u0474\u0003\u008eG\u0000\u0474\u0475\u0005\u0001\u0000\u0000\u0475\u0476"+
		"\u0005\u00e5\u0000\u0000\u0476\u0479\u0001\u0000\u0000\u0000\u0477\u0479"+
		"\u0005\u00e5\u0000\u0000\u0478\u046c\u0001\u0000\u0000\u0000\u0478\u0473"+
		"\u0001\u0000\u0000\u0000\u0478\u0477\u0001\u0000\u0000\u0000\u0479C\u0001"+
		"\u0000\u0000\u0000\u047a\u047b\u0006\"\uffff\uffff\u0000\u047b\u047c\u0003"+
		"J%\u0000\u047c\u048f\u0001\u0000\u0000\u0000\u047d\u048b\n\u0002\u0000"+
		"\u0000\u047e\u047f\u0005%\u0000\u0000\u047f\u0480\u0005f\u0000\u0000\u0480"+
		"\u048c\u0003J%\u0000\u0481\u0482\u0003F#\u0000\u0482\u0483\u0005f\u0000"+
		"\u0000\u0483\u0484\u0003D\"\u0000\u0484\u0485\u0003H$\u0000\u0485\u048c"+
		"\u0001\u0000\u0000\u0000\u0486\u0487\u0005v\u0000\u0000\u0487\u0488\u0003"+
		"F#\u0000\u0488\u0489\u0005f\u0000\u0000\u0489\u048a\u0003J%\u0000\u048a"+
		"\u048c\u0001\u0000\u0000\u0000\u048b\u047e\u0001\u0000\u0000\u0000\u048b"+
		"\u0481\u0001\u0000\u0000\u0000\u048b\u0486\u0001\u0000\u0000\u0000\u048c"+
		"\u048e\u0001\u0000\u0000\u0000\u048d\u047d\u0001\u0000\u0000\u0000\u048e"+
		"\u0491\u0001\u0000\u0000\u0000\u048f\u048d\u0001\u0000\u0000\u0000\u048f"+
		"\u0490\u0001\u0000\u0000\u0000\u0490E\u0001\u0000\u0000\u0000\u0491\u048f"+
		"\u0001\u0000\u0000\u0000\u0492\u0494\u0005[\u0000\u0000\u0493\u0492\u0001"+
		"\u0000\u0000\u0000\u0493\u0494\u0001\u0000\u0000\u0000\u0494\u04a2\u0001"+
		"\u0000\u0000\u0000\u0495\u0497\u0005j\u0000\u0000\u0496\u0498\u0005\u008a"+
		"\u0000\u0000\u0497\u0496\u0001\u0000\u0000\u0000\u0497\u0498\u0001\u0000"+
		"\u0000\u0000\u0498\u04a2\u0001\u0000\u0000\u0000\u0499\u049b\u0005\u00a1"+
		"\u0000\u0000\u049a\u049c\u0005\u008a\u0000\u0000\u049b\u049a\u0001\u0000"+
		"\u0000\u0000\u049b\u049c\u0001\u0000\u0000\u0000\u049c\u04a2\u0001\u0000"+
		"\u0000\u0000\u049d\u049f\u0005K\u0000\u0000\u049e\u04a0\u0005\u008a\u0000"+
		"\u0000\u049f\u049e\u0001\u0000\u0000\u0000\u049f\u04a0\u0001\u0000\u0000"+
		"\u0000\u04a0\u04a2\u0001\u0000\u0000\u0000\u04a1\u0493\u0001\u0000\u0000"+
		"\u0000\u04a1\u0495\u0001\u0000\u0000\u0000\u04a1\u0499\u0001\u0000\u0000"+
		"\u0000\u04a1\u049d\u0001\u0000\u0000\u0000\u04a2G\u0001\u0000\u0000\u0000"+
		"\u04a3\u04a4\u0005\u0084\u0000\u0000\u04a4\u04b2\u0003V+\u0000\u04a5\u04a6"+
		"\u0005\u00d0\u0000\u0000\u04a6\u04a7\u0005\u0002\u0000\u0000\u04a7\u04ac"+
		"\u0003\u0098L\u0000\u04a8\u04a9\u0005\u0004\u0000\u0000\u04a9\u04ab\u0003"+
		"\u0098L\u0000\u04aa\u04a8\u0001\u0000\u0000\u0000\u04ab\u04ae\u0001\u0000"+
		"\u0000\u0000\u04ac\u04aa\u0001\u0000\u0000\u0000\u04ac\u04ad\u0001\u0000"+
		"\u0000\u0000\u04ad\u04af\u0001\u0000\u0000\u0000\u04ae\u04ac\u0001\u0000"+
		"\u0000\u0000\u04af\u04b0\u0005\u0003\u0000\u0000\u04b0\u04b2\u0001\u0000"+
		"\u0000\u0000\u04b1\u04a3\u0001\u0000\u0000\u0000\u04b1\u04a5\u0001\u0000"+
		"\u0000\u0000\u04b2I\u0001\u0000\u0000\u0000\u04b3\u04ba\u0003N\'\u0000"+
		"\u04b4\u04b5\u0005\u00bc\u0000\u0000\u04b5\u04b6\u0003L&\u0000\u04b6\u04b7"+
		"\u0005\u0002\u0000\u0000\u04b7\u04b8\u0003T*\u0000\u04b8\u04b9\u0005\u0003"+
		"\u0000\u0000\u04b9\u04bb\u0001\u0000\u0000\u0000\u04ba\u04b4\u0001\u0000"+
		"\u0000\u0000\u04ba\u04bb\u0001\u0000\u0000\u0000\u04bbK\u0001\u0000\u0000"+
		"\u0000\u04bc\u04bd\u0007\n\u0000\u0000\u04bdM\u0001\u0000\u0000\u0000"+
		"\u04be\u04c6\u0003R)\u0000\u04bf\u04c1\u0005\u0012\u0000\u0000\u04c0\u04bf"+
		"\u0001\u0000\u0000\u0000\u04c0\u04c1\u0001\u0000\u0000\u0000\u04c1\u04c2"+
		"\u0001\u0000\u0000\u0000\u04c2\u04c4\u0003\u0098L\u0000\u04c3\u04c5\u0003"+
		"P(\u0000\u04c4\u04c3\u0001\u0000\u0000\u0000\u04c4\u04c5\u0001\u0000\u0000"+
		"\u0000\u04c5\u04c7\u0001\u0000\u0000\u0000\u04c6\u04c0\u0001\u0000\u0000"+
		"\u0000\u04c6\u04c7\u0001\u0000\u0000\u0000\u04c7O\u0001\u0000\u0000\u0000"+
		"\u04c8\u04c9\u0005\u0002\u0000\u0000\u04c9\u04ce\u0003\u0098L\u0000\u04ca"+
		"\u04cb\u0005\u0004\u0000\u0000\u04cb\u04cd\u0003\u0098L\u0000\u04cc\u04ca"+
		"\u0001\u0000\u0000\u0000\u04cd\u04d0\u0001\u0000\u0000\u0000\u04ce\u04cc"+
		"\u0001\u0000\u0000\u0000\u04ce\u04cf\u0001\u0000\u0000\u0000\u04cf\u04d1"+
		"\u0001\u0000\u0000\u0000\u04d0\u04ce\u0001\u0000\u0000\u0000\u04d1\u04d2"+
		"\u0005\u0003\u0000\u0000\u04d2Q\u0001\u0000\u0000\u0000\u04d3\u04d5\u0003"+
		"\u008eG\u0000\u04d4\u04d6\u0003\u0090H\u0000\u04d5\u04d4\u0001\u0000\u0000"+
		"\u0000\u04d5\u04d6\u0001\u0000\u0000\u0000\u04d6\u04f4\u0001\u0000\u0000"+
		"\u0000\u04d7\u04d8\u0005\u0002\u0000\u0000\u04d8\u04d9\u0003\b\u0004\u0000"+
		"\u04d9\u04da\u0005\u0003\u0000\u0000\u04da\u04f4\u0001\u0000\u0000\u0000"+
		"\u04db\u04dc\u0005\u00cc\u0000\u0000\u04dc\u04dd\u0005\u0002\u0000\u0000"+
		"\u04dd\u04e2\u0003T*\u0000\u04de\u04df\u0005\u0004\u0000\u0000\u04df\u04e1"+
		"\u0003T*\u0000\u04e0\u04de\u0001\u0000\u0000\u0000\u04e1\u04e4\u0001\u0000"+
		"\u0000\u0000\u04e2\u04e0\u0001\u0000\u0000\u0000\u04e2\u04e3\u0001\u0000"+
		"\u0000\u0000\u04e3\u04e5\u0001\u0000\u0000\u0000\u04e4\u04e2\u0001\u0000"+
		"\u0000\u0000\u04e5\u04e8\u0005\u0003\u0000\u0000\u04e6\u04e7\u0005\u00d8"+
		"\u0000\u0000\u04e7\u04e9\u0005\u0089\u0000\u0000\u04e8\u04e6\u0001\u0000"+
		"\u0000\u0000\u04e8\u04e9\u0001\u0000\u0000\u0000\u04e9\u04f4\u0001\u0000"+
		"\u0000\u0000\u04ea\u04eb\u0005i\u0000\u0000\u04eb\u04ec\u0005\u0002\u0000"+
		"\u0000\u04ec\u04ed\u0003\b\u0004\u0000\u04ed\u04ee\u0005\u0003\u0000\u0000"+
		"\u04ee\u04f4\u0001\u0000\u0000\u0000\u04ef\u04f0\u0005\u0002\u0000\u0000"+
		"\u04f0\u04f1\u0003D\"\u0000\u04f1\u04f2\u0005\u0003\u0000\u0000\u04f2"+
		"\u04f4\u0001\u0000\u0000\u0000\u04f3\u04d3\u0001\u0000\u0000\u0000\u04f3"+
		"\u04d7\u0001\u0000\u0000\u0000\u04f3\u04db\u0001\u0000\u0000\u0000\u04f3"+
		"\u04ea\u0001\u0000\u0000\u0000\u04f3\u04ef\u0001\u0000\u0000\u0000\u04f4"+
		"S\u0001\u0000\u0000\u0000\u04f5\u04f6\u0003V+\u0000\u04f6U\u0001\u0000"+
		"\u0000\u0000\u04f7\u04f8\u0006+\uffff\uffff\u0000\u04f8\u04fa\u0003Z-"+
		"\u0000\u04f9\u04fb\u0003X,\u0000\u04fa\u04f9\u0001\u0000\u0000\u0000\u04fa"+
		"\u04fb\u0001\u0000\u0000\u0000\u04fb\u04ff\u0001\u0000\u0000\u0000\u04fc"+
		"\u04fd\u0005~\u0000\u0000\u04fd\u04ff\u0003V+\u0003\u04fe\u04f7\u0001"+
		"\u0000\u0000\u0000\u04fe\u04fc\u0001\u0000\u0000\u0000\u04ff\u0508\u0001"+
		"\u0000\u0000\u0000\u0500\u0501\n\u0002\u0000\u0000\u0501\u0502\u0005\u000f"+
		"\u0000\u0000\u0502\u0507\u0003V+\u0003\u0503\u0504\n\u0001\u0000\u0000"+
		"\u0504\u0505\u0005\u0087\u0000\u0000\u0505\u0507\u0003V+\u0002\u0506\u0500"+
		"\u0001\u0000\u0000\u0000\u0506\u0503\u0001\u0000\u0000\u0000\u0507\u050a"+
		"\u0001\u0000\u0000\u0000\u0508\u0506\u0001\u0000\u0000\u0000\u0508\u0509"+
		"\u0001\u0000\u0000\u0000\u0509W\u0001\u0000\u0000\u0000\u050a\u0508\u0001"+
		"\u0000\u0000\u0000\u050b\u050c\u0003d2\u0000\u050c\u050d\u0003Z-\u0000"+
		"\u050d\u0549\u0001\u0000\u0000\u0000\u050e\u050f\u0003d2\u0000\u050f\u0510"+
		"\u0003f3\u0000\u0510\u0511\u0005\u0002\u0000\u0000\u0511\u0512\u0003\b"+
		"\u0004\u0000\u0512\u0513\u0005\u0003\u0000\u0000\u0513\u0549\u0001\u0000"+
		"\u0000\u0000\u0514\u0516\u0005~\u0000\u0000\u0515\u0514\u0001\u0000\u0000"+
		"\u0000\u0515\u0516\u0001\u0000\u0000\u0000\u0516\u0517\u0001\u0000\u0000"+
		"\u0000\u0517\u0518\u0005\u0016\u0000\u0000\u0518\u0519\u0003Z-\u0000\u0519"+
		"\u051a\u0005\u000f\u0000\u0000\u051a\u051b\u0003Z-\u0000\u051b\u0549\u0001"+
		"\u0000\u0000\u0000\u051c\u051e\u0005~\u0000\u0000\u051d\u051c\u0001\u0000"+
		"\u0000\u0000\u051d\u051e\u0001\u0000\u0000\u0000\u051e\u051f\u0001\u0000"+
		"\u0000\u0000\u051f\u0520\u0005Y\u0000\u0000\u0520\u0521\u0005\u0002\u0000"+
		"\u0000\u0521\u0526\u0003T*\u0000\u0522\u0523\u0005\u0004\u0000\u0000\u0523"+
		"\u0525\u0003T*\u0000\u0524\u0522\u0001\u0000\u0000\u0000\u0525\u0528\u0001"+
		"\u0000\u0000\u0000\u0526\u0524\u0001\u0000\u0000\u0000\u0526\u0527\u0001"+
		"\u0000\u0000\u0000\u0527\u0529\u0001\u0000\u0000\u0000\u0528\u0526\u0001"+
		"\u0000\u0000\u0000\u0529\u052a\u0005\u0003\u0000\u0000\u052a\u0549\u0001"+
		"\u0000\u0000\u0000\u052b\u052d\u0005~\u0000\u0000\u052c\u052b\u0001\u0000"+
		"\u0000\u0000\u052c\u052d\u0001\u0000\u0000\u0000\u052d\u052e\u0001\u0000"+
		"\u0000\u0000\u052e\u052f\u0005Y\u0000\u0000\u052f\u0530\u0005\u0002\u0000"+
		"\u0000\u0530\u0531\u0003\b\u0004\u0000\u0531\u0532\u0005\u0003\u0000\u0000"+
		"\u0532\u0549\u0001\u0000\u0000\u0000\u0533\u0535\u0005~\u0000\u0000\u0534"+
		"\u0533\u0001\u0000\u0000\u0000\u0534\u0535\u0001\u0000\u0000\u0000\u0535"+
		"\u0536\u0001\u0000\u0000\u0000\u0536\u0537\u0005l\u0000\u0000\u0537\u053a"+
		"\u0003Z-\u0000\u0538\u0539\u0005;\u0000\u0000\u0539\u053b\u0003Z-\u0000"+
		"\u053a\u0538\u0001\u0000\u0000\u0000\u053a\u053b\u0001\u0000\u0000\u0000"+
		"\u053b\u0549\u0001\u0000\u0000\u0000\u053c\u053e\u0005c\u0000\u0000\u053d"+
		"\u053f\u0005~\u0000\u0000\u053e\u053d\u0001\u0000\u0000\u0000\u053e\u053f"+
		"\u0001\u0000\u0000\u0000\u053f\u0540\u0001\u0000\u0000\u0000\u0540\u0549"+
		"\u0005\u007f\u0000\u0000\u0541\u0543\u0005c\u0000\u0000\u0542\u0544\u0005"+
		"~\u0000\u0000\u0543\u0542\u0001\u0000\u0000\u0000\u0543\u0544\u0001\u0000"+
		"\u0000\u0000\u0544\u0545\u0001\u0000\u0000\u0000\u0545\u0546\u00056\u0000"+
		"\u0000\u0546\u0547\u0005J\u0000\u0000\u0547\u0549\u0003Z-\u0000\u0548"+
		"\u050b\u0001\u0000\u0000\u0000\u0548\u050e\u0001\u0000\u0000\u0000\u0548"+
		"\u0515\u0001\u0000\u0000\u0000\u0548\u051d\u0001\u0000\u0000\u0000\u0548"+
		"\u052c\u0001\u0000\u0000\u0000\u0548\u0534\u0001\u0000\u0000\u0000\u0548"+
		"\u053c\u0001\u0000\u0000\u0000\u0548\u0541\u0001\u0000\u0000\u0000\u0549"+
		"Y\u0001\u0000\u0000\u0000\u054a\u054b\u0006-\uffff\uffff\u0000\u054b\u054f"+
		"\u0003\\.\u0000\u054c\u054d\u0007\u000b\u0000\u0000\u054d\u054f\u0003"+
		"Z-\u0004\u054e\u054a\u0001\u0000\u0000\u0000\u054e\u054c\u0001\u0000\u0000"+
		"\u0000\u054f\u055e\u0001\u0000\u0000\u0000\u0550\u0551\n\u0003\u0000\u0000"+
		"\u0551\u0552\u0007\f\u0000\u0000\u0552\u055d\u0003Z-\u0004\u0553\u0554"+
		"\n\u0002\u0000\u0000\u0554\u0555\u0007\u000b\u0000\u0000\u0555\u055d\u0003"+
		"Z-\u0003\u0556\u0557\n\u0001\u0000\u0000\u0557\u0558\u0005\u00e8\u0000"+
		"\u0000\u0558\u055d\u0003Z-\u0002\u0559\u055a\n\u0005\u0000\u0000\u055a"+
		"\u055b\u0005\u0014\u0000\u0000\u055b\u055d\u0003b1\u0000\u055c\u0550\u0001"+
		"\u0000\u0000\u0000\u055c\u0553\u0001\u0000\u0000\u0000\u055c\u0556\u0001"+
		"\u0000\u0000\u0000\u055c\u0559\u0001\u0000\u0000\u0000\u055d\u0560\u0001"+
		"\u0000\u0000\u0000\u055e\u055c\u0001\u0000\u0000\u0000\u055e\u055f\u0001"+
		"\u0000\u0000\u0000\u055f[\u0001\u0000\u0000\u0000\u0560\u055e\u0001\u0000"+
		"\u0000\u0000\u0561\u0562\u0006.\uffff\uffff\u0000\u0562\u0651\u0005\u007f"+
		"\u0000\u0000\u0563\u0651\u0003j5\u0000\u0564\u0565\u0003\u0098L\u0000"+
		"\u0565\u0566\u0003^/\u0000\u0566\u0651\u0001\u0000\u0000\u0000\u0567\u0568"+
		"\u0005\u00f5\u0000\u0000\u0568\u0651\u0003^/\u0000\u0569\u0651\u0003\u009a"+
		"M\u0000\u056a\u0651\u0003h4\u0000\u056b\u0651\u0003^/\u0000\u056c\u0651"+
		"\u0005\u00eb\u0000\u0000\u056d\u0651\u0005\u0005\u0000\u0000\u056e\u056f"+
		"\u0005\u008f\u0000\u0000\u056f\u0570\u0005\u0002\u0000\u0000\u0570\u0571"+
		"\u0003Z-\u0000\u0571\u0572\u0005Y\u0000\u0000\u0572\u0573\u0003Z-\u0000"+
		"\u0573\u0574\u0005\u0003\u0000\u0000\u0574\u0651\u0001\u0000\u0000\u0000"+
		"\u0575\u0576\u0005\u0002\u0000\u0000\u0576\u0579\u0003T*\u0000\u0577\u0578"+
		"\u0005\u0004\u0000\u0000\u0578\u057a\u0003T*\u0000\u0579\u0577\u0001\u0000"+
		"\u0000\u0000\u057a\u057b\u0001\u0000\u0000\u0000\u057b\u0579\u0001\u0000"+
		"\u0000\u0000\u057b\u057c\u0001\u0000\u0000\u0000\u057c\u057d\u0001\u0000"+
		"\u0000\u0000\u057d\u057e\u0005\u0003\u0000\u0000\u057e\u0651\u0001\u0000"+
		"\u0000\u0000\u057f\u0580\u0005\u00a6\u0000\u0000\u0580\u0581\u0005\u0002"+
		"\u0000\u0000\u0581\u0586\u0003T*\u0000\u0582\u0583\u0005\u0004\u0000\u0000"+
		"\u0583\u0585\u0003T*\u0000\u0584\u0582\u0001\u0000\u0000\u0000\u0585\u0588"+
		"\u0001\u0000\u0000\u0000\u0586\u0584\u0001\u0000\u0000\u0000\u0586\u0587"+
		"\u0001\u0000\u0000\u0000\u0587\u0589\u0001\u0000\u0000\u0000\u0588\u0586"+
		"\u0001\u0000\u0000\u0000\u0589\u058a\u0005\u0003\u0000\u0000\u058a\u0651"+
		"\u0001\u0000\u0000\u0000\u058b\u058c\u0003\u008eG\u0000\u058c\u058d\u0005"+
		"\u0002\u0000\u0000\u058d\u058e\u0005\u00e5\u0000\u0000\u058e\u0590\u0005"+
		"\u0003\u0000\u0000\u058f\u0591\u0003z=\u0000\u0590\u058f\u0001\u0000\u0000"+
		"\u0000\u0590\u0591\u0001\u0000\u0000\u0000\u0591\u0593\u0001\u0000\u0000"+
		"\u0000\u0592\u0594\u0003|>\u0000\u0593\u0592\u0001\u0000\u0000\u0000\u0593"+
		"\u0594\u0001\u0000\u0000\u0000\u0594\u0651\u0001\u0000\u0000\u0000\u0595"+
		"\u0596\u0003\u008eG\u0000\u0596\u05a2\u0005\u0002\u0000\u0000\u0597\u0599"+
		"\u0003@ \u0000\u0598\u0597\u0001\u0000\u0000\u0000\u0598\u0599\u0001\u0000"+
		"\u0000\u0000\u0599\u059a\u0001\u0000\u0000\u0000\u059a\u059f\u0003T*\u0000"+
		"\u059b\u059c\u0005\u0004\u0000\u0000\u059c\u059e\u0003T*\u0000\u059d\u059b"+
		"\u0001\u0000\u0000\u0000\u059e\u05a1\u0001\u0000\u0000\u0000\u059f\u059d"+
		"\u0001\u0000\u0000\u0000\u059f\u05a0\u0001\u0000\u0000\u0000\u05a0\u05a3"+
		"\u0001\u0000\u0000\u0000\u05a1\u059f\u0001\u0000\u0000\u0000\u05a2\u0598"+
		"\u0001\u0000\u0000\u0000\u05a2\u05a3\u0001\u0000\u0000\u0000\u05a3\u05ae"+
		"\u0001\u0000\u0000\u0000\u05a4\u05a5\u0005\u0088\u0000\u0000\u05a5\u05a6"+
		"\u0005\u0017\u0000\u0000\u05a6\u05ab\u00034\u001a\u0000\u05a7\u05a8\u0005"+
		"\u0004\u0000\u0000\u05a8\u05aa\u00034\u001a\u0000\u05a9\u05a7\u0001\u0000"+
		"\u0000\u0000\u05aa\u05ad\u0001\u0000\u0000\u0000\u05ab\u05a9\u0001\u0000"+
		"\u0000\u0000\u05ab\u05ac\u0001\u0000\u0000\u0000\u05ac\u05af\u0001\u0000"+
		"\u0000\u0000\u05ad\u05ab\u0001\u0000\u0000\u0000\u05ae\u05a4\u0001\u0000"+
		"\u0000\u0000\u05ae\u05af\u0001\u0000\u0000\u0000\u05af\u05b0\u0001\u0000"+
		"\u0000\u0000\u05b0\u05b2\u0005\u0003\u0000\u0000\u05b1\u05b3\u0003z=\u0000"+
		"\u05b2\u05b1\u0001\u0000\u0000\u0000\u05b2\u05b3\u0001\u0000\u0000\u0000"+
		"\u05b3\u05b8\u0001\u0000\u0000\u0000\u05b4\u05b6\u0003`0\u0000\u05b5\u05b4"+
		"\u0001\u0000\u0000\u0000\u05b5\u05b6\u0001\u0000\u0000\u0000\u05b6\u05b7"+
		"\u0001\u0000\u0000\u0000\u05b7\u05b9\u0003|>\u0000\u05b8\u05b5\u0001\u0000"+
		"\u0000\u0000\u05b8\u05b9\u0001\u0000\u0000\u0000\u05b9\u0651\u0001\u0000"+
		"\u0000\u0000\u05ba\u05bb\u0003\u0098L\u0000\u05bb\u05bc\u0005\u0006\u0000"+
		"\u0000\u05bc\u05bd\u0003T*\u0000\u05bd\u0651\u0001\u0000\u0000\u0000\u05be"+
		"\u05c7\u0005\u0002\u0000\u0000\u05bf\u05c4\u0003\u0098L\u0000\u05c0\u05c1"+
		"\u0005\u0004\u0000\u0000\u05c1\u05c3\u0003\u0098L\u0000\u05c2\u05c0\u0001"+
		"\u0000\u0000\u0000\u05c3\u05c6\u0001\u0000\u0000\u0000\u05c4\u05c2\u0001"+
		"\u0000\u0000\u0000\u05c4\u05c5\u0001\u0000\u0000\u0000\u05c5\u05c8\u0001"+
		"\u0000\u0000\u0000\u05c6\u05c4\u0001\u0000\u0000\u0000\u05c7\u05bf\u0001"+
		"\u0000\u0000\u0000\u05c7\u05c8\u0001\u0000\u0000\u0000\u05c8\u05c9\u0001"+
		"\u0000\u0000\u0000\u05c9\u05ca\u0005\u0003\u0000\u0000\u05ca\u05cb\u0005"+
		"\u0006\u0000\u0000\u05cb\u0651\u0003T*\u0000\u05cc\u05cd\u0005\u0002\u0000"+
		"\u0000\u05cd\u05ce\u0003\b\u0004\u0000\u05ce\u05cf\u0005\u0003\u0000\u0000"+
		"\u05cf\u0651\u0001\u0000\u0000\u0000\u05d0\u05d1\u0005?\u0000\u0000\u05d1"+
		"\u05d2\u0005\u0002\u0000\u0000\u05d2\u05d3\u0003\b\u0004\u0000\u05d3\u05d4"+
		"\u0005\u0003\u0000\u0000\u05d4\u0651\u0001\u0000\u0000\u0000\u05d5\u05d6"+
		"\u0005\u001b\u0000\u0000\u05d6\u05d8\u0003Z-\u0000\u05d7\u05d9\u0003x"+
		"<\u0000\u05d8\u05d7\u0001\u0000\u0000\u0000\u05d9\u05da\u0001\u0000\u0000"+
		"\u0000\u05da\u05d8\u0001\u0000\u0000\u0000\u05da\u05db\u0001\u0000\u0000"+
		"\u0000\u05db\u05de\u0001\u0000\u0000\u0000\u05dc\u05dd\u00059\u0000\u0000"+
		"\u05dd\u05df\u0003T*\u0000\u05de\u05dc\u0001\u0000\u0000\u0000\u05de\u05df"+
		"\u0001\u0000\u0000\u0000\u05df\u05e0\u0001\u0000\u0000\u0000\u05e0\u05e1"+
		"\u0005:\u0000\u0000\u05e1\u0651\u0001\u0000\u0000\u0000\u05e2\u05e4\u0005"+
		"\u001b\u0000\u0000\u05e3\u05e5\u0003x<\u0000\u05e4\u05e3\u0001\u0000\u0000"+
		"\u0000\u05e5\u05e6\u0001\u0000\u0000\u0000\u05e6\u05e4\u0001\u0000\u0000"+
		"\u0000\u05e6\u05e7\u0001\u0000\u0000\u0000\u05e7\u05ea\u0001\u0000\u0000"+
		"\u0000\u05e8\u05e9\u00059\u0000\u0000\u05e9\u05eb\u0003T*\u0000\u05ea"+
		"\u05e8\u0001\u0000\u0000\u0000\u05ea\u05eb\u0001\u0000\u0000\u0000\u05eb"+
		"\u05ec\u0001\u0000\u0000\u0000\u05ec\u05ed\u0005:\u0000\u0000\u05ed\u0651"+
		"\u0001\u0000\u0000\u0000\u05ee\u05ef\u0005\u001c\u0000\u0000\u05ef\u05f0"+
		"\u0005\u0002\u0000\u0000\u05f0\u05f1\u0003T*\u0000\u05f1\u05f2\u0005\u0012"+
		"\u0000\u0000\u05f2\u05f3\u0003r9\u0000\u05f3\u05f4\u0005\u0003\u0000\u0000"+
		"\u05f4\u0651\u0001\u0000\u0000\u0000\u05f5\u05f6\u0005\u00c6\u0000\u0000"+
		"\u05f6\u05f7\u0005\u0002\u0000\u0000\u05f7\u05f8\u0003T*\u0000\u05f8\u05f9"+
		"\u0005\u0012\u0000\u0000\u05f9\u05fa\u0003r9\u0000\u05fa\u05fb\u0005\u0003"+
		"\u0000\u0000\u05fb\u0651\u0001\u0000\u0000\u0000\u05fc\u05fd\u0005\u0011"+
		"\u0000\u0000\u05fd\u0606\u0005\u0007\u0000\u0000\u05fe\u0603\u0003T*\u0000"+
		"\u05ff\u0600\u0005\u0004\u0000\u0000\u0600\u0602\u0003T*\u0000\u0601\u05ff"+
		"\u0001\u0000\u0000\u0000\u0602\u0605\u0001\u0000\u0000\u0000\u0603\u0601"+
		"\u0001\u0000\u0000\u0000\u0603\u0604\u0001\u0000\u0000\u0000\u0604\u0607"+
		"\u0001\u0000\u0000\u0000\u0605\u0603\u0001\u0000\u0000\u0000\u0606\u05fe"+
		"\u0001\u0000\u0000\u0000\u0606\u0607\u0001\u0000\u0000\u0000\u0607\u0608"+
		"\u0001\u0000\u0000\u0000\u0608\u0651\u0005\b\u0000\u0000\u0609\u0651\u0003"+
		"\u0098L\u0000\u060a\u0651\u0005(\u0000\u0000\u060b\u060f\u0005*\u0000"+
		"\u0000\u060c\u060d\u0005\u0002\u0000\u0000\u060d\u060e\u0005\u00ec\u0000"+
		"\u0000\u060e\u0610\u0005\u0003\u0000\u0000\u060f\u060c\u0001\u0000\u0000"+
		"\u0000\u060f\u0610\u0001\u0000\u0000\u0000\u0610\u0651\u0001\u0000\u0000"+
		"\u0000\u0611\u0615\u0005+\u0000\u0000\u0612\u0613\u0005\u0002\u0000\u0000"+
		"\u0613\u0614\u0005\u00ec\u0000\u0000\u0614\u0616\u0005\u0003\u0000\u0000"+
		"\u0615\u0612\u0001\u0000\u0000\u0000\u0615\u0616\u0001\u0000\u0000\u0000"+
		"\u0616\u0651\u0001\u0000\u0000\u0000\u0617\u061b\u0005n\u0000\u0000\u0618"+
		"\u0619\u0005\u0002\u0000\u0000\u0619\u061a\u0005\u00ec\u0000\u0000\u061a"+
		"\u061c\u0005\u0003\u0000\u0000\u061b\u0618\u0001\u0000\u0000\u0000\u061b"+
		"\u061c\u0001\u0000\u0000\u0000\u061c\u0651\u0001\u0000\u0000\u0000\u061d"+
		"\u0621\u0005o\u0000\u0000\u061e\u061f\u0005\u0002\u0000\u0000\u061f\u0620"+
		"\u0005\u00ec\u0000\u0000\u0620\u0622\u0005\u0003\u0000\u0000\u0621\u061e"+
		"\u0001\u0000\u0000\u0000\u0621\u0622\u0001\u0000\u0000\u0000\u0622\u0651"+
		"\u0001\u0000\u0000\u0000\u0623\u0651\u0005,\u0000\u0000\u0624\u0625\u0005"+
		"\u00b6\u0000\u0000\u0625\u0626\u0005\u0002\u0000\u0000\u0626\u0627\u0003"+
		"Z-\u0000\u0627\u0628\u0005J\u0000\u0000\u0628\u062b\u0003Z-\u0000\u0629"+
		"\u062a\u0005H\u0000\u0000\u062a\u062c\u0003Z-\u0000\u062b\u0629\u0001"+
		"\u0000\u0000\u0000\u062b\u062c\u0001\u0000\u0000\u0000\u062c\u062d\u0001"+
		"\u0000\u0000\u0000\u062d\u062e\u0005\u0003\u0000\u0000\u062e\u0651\u0001"+
		"\u0000\u0000\u0000\u062f\u0630\u0005}\u0000\u0000\u0630\u0631\u0005\u0002"+
		"\u0000\u0000\u0631\u0634\u0003Z-\u0000\u0632\u0633\u0005\u0004\u0000\u0000"+
		"\u0633\u0635\u0003n7\u0000\u0634\u0632\u0001\u0000\u0000\u0000\u0634\u0635"+
		"\u0001\u0000\u0000\u0000\u0635\u0636\u0001\u0000\u0000\u0000\u0636\u0637"+
		"\u0005\u0003\u0000\u0000\u0637\u0651\u0001\u0000\u0000\u0000\u0638\u0639"+
		"\u0005A\u0000\u0000\u0639\u063a\u0005\u0002\u0000\u0000\u063a\u063b\u0003"+
		"\u0098L\u0000\u063b\u063c\u0005J\u0000\u0000\u063c\u063d\u0003Z-\u0000"+
		"\u063d\u063e\u0005\u0003\u0000\u0000\u063e\u0651\u0001\u0000\u0000\u0000"+
		"\u063f\u0640\u0005\u0002\u0000\u0000\u0640\u0641\u0003T*\u0000\u0641\u0642"+
		"\u0005\u0003\u0000\u0000\u0642\u0651\u0001\u0000\u0000\u0000\u0643\u0644"+
		"\u0005S\u0000\u0000\u0644\u064d\u0005\u0002\u0000\u0000\u0645\u064a\u0003"+
		"\u008eG\u0000\u0646\u0647\u0005\u0004\u0000\u0000\u0647\u0649\u0003\u008e"+
		"G\u0000\u0648\u0646\u0001\u0000\u0000\u0000\u0649\u064c\u0001\u0000\u0000"+
		"\u0000\u064a\u0648\u0001\u0000\u0000\u0000\u064a\u064b\u0001\u0000\u0000"+
		"\u0000\u064b\u064e\u0001\u0000\u0000\u0000\u064c\u064a\u0001\u0000\u0000"+
		"\u0000\u064d\u0645\u0001\u0000\u0000\u0000\u064d\u064e\u0001\u0000\u0000"+
		"\u0000\u064e\u064f\u0001\u0000\u0000\u0000\u064f\u0651\u0005\u0003\u0000"+
		"\u0000\u0650\u0561\u0001\u0000\u0000\u0000\u0650\u0563\u0001\u0000\u0000"+
		"\u0000\u0650\u0564\u0001\u0000\u0000\u0000\u0650\u0567\u0001\u0000\u0000"+
		"\u0000\u0650\u0569\u0001\u0000\u0000\u0000\u0650\u056a\u0001\u0000\u0000"+
		"\u0000\u0650\u056b\u0001\u0000\u0000\u0000\u0650\u056c\u0001\u0000\u0000"+
		"\u0000\u0650\u056d\u0001\u0000\u0000\u0000\u0650\u056e\u0001\u0000\u0000"+
		"\u0000\u0650\u0575\u0001\u0000\u0000\u0000\u0650\u057f\u0001\u0000\u0000"+
		"\u0000\u0650\u058b\u0001\u0000\u0000\u0000\u0650\u0595\u0001\u0000\u0000"+
		"\u0000\u0650\u05ba\u0001\u0000\u0000\u0000\u0650\u05be\u0001\u0000\u0000"+
		"\u0000\u0650\u05cc\u0001\u0000\u0000\u0000\u0650\u05d0\u0001\u0000\u0000"+
		"\u0000\u0650\u05d5\u0001\u0000\u0000\u0000\u0650\u05e2\u0001\u0000\u0000"+
		"\u0000\u0650\u05ee\u0001\u0000\u0000\u0000\u0650\u05f5\u0001\u0000\u0000"+
		"\u0000\u0650\u05fc\u0001\u0000\u0000\u0000\u0650\u0609\u0001\u0000\u0000"+
		"\u0000\u0650\u060a\u0001\u0000\u0000\u0000\u0650\u060b\u0001\u0000\u0000"+
		"\u0000\u0650\u0611\u0001\u0000\u0000\u0000\u0650\u0617\u0001\u0000\u0000"+
		"\u0000\u0650\u061d\u0001\u0000\u0000\u0000\u0650\u0623\u0001\u0000\u0000"+
		"\u0000\u0650\u0624\u0001\u0000\u0000\u0000\u0650\u062f\u0001\u0000\u0000"+
		"\u0000\u0650\u0638\u0001\u0000\u0000\u0000\u0650\u063f\u0001\u0000\u0000"+
		"\u0000\u0650\u0643\u0001\u0000\u0000\u0000\u0651\u065c\u0001\u0000\u0000"+
		"\u0000\u0652\u0653\n\u000e\u0000\u0000\u0653\u0654\u0005\u0007\u0000\u0000"+
		"\u0654\u0655\u0003Z-\u0000\u0655\u0656\u0005\b\u0000\u0000\u0656\u065b"+
		"\u0001\u0000\u0000\u0000\u0657\u0658\n\f\u0000\u0000\u0658\u0659\u0005"+
		"\u0001\u0000\u0000\u0659\u065b\u0003\u0098L\u0000\u065a\u0652\u0001\u0000"+
		"\u0000\u0000\u065a\u0657\u0001\u0000\u0000\u0000\u065b\u065e\u0001\u0000"+
		"\u0000\u0000\u065c\u065a\u0001\u0000\u0000\u0000\u065c\u065d\u0001\u0000"+
		"\u0000\u0000\u065d]\u0001\u0000\u0000\u0000\u065e\u065c\u0001\u0000\u0000"+
		"\u0000\u065f\u0666\u0005\u00e9\u0000\u0000\u0660\u0663\u0005\u00ea\u0000"+
		"\u0000\u0661\u0662\u0005\u00c8\u0000\u0000\u0662\u0664\u0005\u00e9\u0000"+
		"\u0000\u0663\u0661\u0001\u0000\u0000\u0000\u0663\u0664\u0001\u0000\u0000"+
		"\u0000\u0664\u0666\u0001\u0000\u0000\u0000\u0665\u065f\u0001\u0000\u0000"+
		"\u0000\u0665\u0660\u0001\u0000\u0000\u0000\u0666_\u0001\u0000\u0000\u0000"+
		"\u0667\u0668\u0005X\u0000\u0000\u0668\u066c\u0005\u0081\u0000\u0000\u0669"+
		"\u066a\u0005\u009c\u0000\u0000\u066a\u066c\u0005\u0081\u0000\u0000\u066b"+
		"\u0667\u0001\u0000\u0000\u0000\u066b\u0669\u0001\u0000\u0000\u0000\u066c"+
		"a\u0001\u0000\u0000\u0000\u066d\u066e\u0005\u00c0\u0000\u0000\u066e\u066f"+
		"\u0005\u00dc\u0000\u0000\u066f\u0674\u0003j5\u0000\u0670\u0671\u0005\u00c0"+
		"\u0000\u0000\u0671\u0672\u0005\u00dc\u0000\u0000\u0672\u0674\u0003^/\u0000"+
		"\u0673\u066d\u0001\u0000\u0000\u0000\u0673\u0670\u0001\u0000\u0000\u0000"+
		"\u0674c\u0001\u0000\u0000\u0000\u0675\u0676\u0007\r\u0000\u0000\u0676"+
		"e\u0001\u0000\u0000\u0000\u0677\u0678\u0007\u000e\u0000\u0000\u0678g\u0001"+
		"\u0000\u0000\u0000\u0679\u067a\u0007\u000f\u0000\u0000\u067ai\u0001\u0000"+
		"\u0000\u0000\u067b\u067d\u0005_\u0000\u0000\u067c\u067e\u0007\u000b\u0000"+
		"\u0000\u067d\u067c\u0001\u0000\u0000\u0000\u067d\u067e\u0001\u0000\u0000"+
		"\u0000\u067e\u067f\u0001\u0000\u0000\u0000\u067f\u0680\u0003^/\u0000\u0680"+
		"\u0683\u0003l6\u0000\u0681\u0682\u0005\u00c2\u0000\u0000\u0682\u0684\u0003"+
		"l6\u0000\u0683\u0681\u0001\u0000\u0000\u0000\u0683\u0684\u0001\u0000\u0000"+
		"\u0000\u0684k\u0001\u0000\u0000\u0000\u0685\u0686\u0007\u0010\u0000\u0000"+
		"\u0686m\u0001\u0000\u0000\u0000\u0687\u0688\u0007\u0011\u0000\u0000\u0688"+
		"o\u0001\u0000\u0000\u0000\u0689\u0692\u0005\u0002\u0000\u0000\u068a\u068f"+
		"\u0003r9\u0000\u068b\u068c\u0005\u0004\u0000\u0000\u068c\u068e\u0003r"+
		"9\u0000\u068d\u068b\u0001\u0000\u0000\u0000\u068e\u0691\u0001\u0000\u0000"+
		"\u0000\u068f\u068d\u0001\u0000\u0000\u0000\u068f\u0690\u0001\u0000\u0000"+
		"\u0000\u0690\u0693\u0001\u0000\u0000\u0000\u0691\u068f\u0001\u0000\u0000"+
		"\u0000\u0692\u068a\u0001\u0000\u0000\u0000\u0692\u0693\u0001\u0000\u0000"+
		"\u0000\u0693\u0694\u0001\u0000\u0000\u0000\u0694\u0695\u0005\u0003\u0000"+
		"\u0000\u0695q\u0001\u0000\u0000\u0000\u0696\u0697\u00069\uffff\uffff\u0000"+
		"\u0697\u0698\u0005\u0011\u0000\u0000\u0698\u0699\u0005\u00df\u0000\u0000"+
		"\u0699\u069a\u0003r9\u0000\u069a\u069b\u0005\u00e1\u0000\u0000\u069b\u06c6"+
		"\u0001\u0000\u0000\u0000\u069c\u069d\u0005q\u0000\u0000\u069d\u069e\u0005"+
		"\u00df\u0000\u0000\u069e\u069f\u0003r9\u0000\u069f\u06a0\u0005\u0004\u0000"+
		"\u0000\u06a0\u06a1\u0003r9\u0000\u06a1\u06a2\u0005\u00e1\u0000\u0000\u06a2"+
		"\u06c6\u0001\u0000\u0000\u0000\u06a3\u06a4\u0005\u00a6\u0000\u0000\u06a4"+
		"\u06a5\u0005\u0002\u0000\u0000\u06a5\u06a6\u0003\u0098L\u0000\u06a6\u06ad"+
		"\u0003r9\u0000\u06a7\u06a8\u0005\u0004\u0000\u0000\u06a8\u06a9\u0003\u0098"+
		"L\u0000\u06a9\u06aa\u0003r9\u0000\u06aa\u06ac\u0001\u0000\u0000\u0000"+
		"\u06ab\u06a7\u0001\u0000\u0000\u0000\u06ac\u06af\u0001\u0000\u0000\u0000"+
		"\u06ad\u06ab\u0001\u0000\u0000\u0000\u06ad\u06ae\u0001\u0000\u0000\u0000"+
		"\u06ae\u06b0\u0001\u0000\u0000\u0000\u06af\u06ad\u0001\u0000\u0000\u0000"+
		"\u06b0\u06b1\u0005\u0003\u0000\u0000\u06b1\u06c6\u0001\u0000\u0000\u0000"+
		"\u06b2\u06be\u0003v;\u0000\u06b3\u06b4\u0005\u0002\u0000\u0000\u06b4\u06b9"+
		"\u0003t:\u0000\u06b5\u06b6\u0005\u0004\u0000\u0000\u06b6\u06b8\u0003t"+
		":\u0000\u06b7\u06b5\u0001\u0000\u0000\u0000\u06b8\u06bb\u0001\u0000\u0000"+
		"\u0000\u06b9\u06b7\u0001\u0000\u0000\u0000\u06b9\u06ba\u0001\u0000\u0000"+
		"\u0000\u06ba\u06bc\u0001\u0000\u0000\u0000\u06bb\u06b9\u0001\u0000\u0000"+
		"\u0000\u06bc\u06bd\u0005\u0003\u0000\u0000\u06bd\u06bf\u0001\u0000\u0000"+
		"\u0000\u06be\u06b3\u0001\u0000\u0000\u0000\u06be\u06bf\u0001\u0000\u0000"+
		"\u0000\u06bf\u06c6\u0001\u0000\u0000\u0000\u06c0\u06c1\u0005_\u0000\u0000"+
		"\u06c1\u06c2\u0003l6\u0000\u06c2\u06c3\u0005\u00c2\u0000\u0000\u06c3\u06c4"+
		"\u0003l6\u0000\u06c4\u06c6\u0001\u0000\u0000\u0000\u06c5\u0696\u0001\u0000"+
		"\u0000\u0000\u06c5\u069c\u0001\u0000\u0000\u0000\u06c5\u06a3\u0001\u0000"+
		"\u0000\u0000\u06c5\u06b2\u0001\u0000\u0000\u0000\u06c5\u06c0\u0001\u0000"+
		"\u0000\u0000\u06c6\u06cb\u0001\u0000\u0000\u0000\u06c7\u06c8\n\u0006\u0000"+
		"\u0000\u06c8\u06ca\u0005\u0011\u0000\u0000\u06c9\u06c7\u0001\u0000\u0000"+
		"\u0000\u06ca\u06cd\u0001\u0000\u0000\u0000\u06cb\u06c9\u0001\u0000\u0000"+
		"\u0000\u06cb\u06cc\u0001\u0000\u0000\u0000\u06ccs\u0001\u0000\u0000\u0000"+
		"\u06cd\u06cb\u0001\u0000\u0000\u0000\u06ce\u06d1\u0005\u00ec\u0000\u0000"+
		"\u06cf\u06d1\u0003r9\u0000\u06d0\u06ce\u0001\u0000\u0000\u0000\u06d0\u06cf"+
		"\u0001\u0000\u0000\u0000\u06d1u\u0001\u0000\u0000\u0000\u06d2\u06d7\u0005"+
		"\u00f3\u0000\u0000\u06d3\u06d7\u0005\u00f4\u0000\u0000\u06d4\u06d7\u0005"+
		"\u00f5\u0000\u0000\u06d5\u06d7\u0003\u008eG\u0000\u06d6\u06d2\u0001\u0000"+
		"\u0000\u0000\u06d6\u06d3\u0001\u0000\u0000\u0000\u06d6\u06d4\u0001\u0000"+
		"\u0000\u0000\u06d6\u06d5\u0001\u0000\u0000\u0000\u06d7w\u0001\u0000\u0000"+
		"\u0000\u06d8\u06d9\u0005\u00d6\u0000\u0000\u06d9\u06da\u0003T*\u0000\u06da"+
		"\u06db\u0005\u00bf\u0000\u0000\u06db\u06dc\u0003T*\u0000\u06dcy\u0001"+
		"\u0000\u0000\u0000\u06dd\u06de\u0005E\u0000\u0000\u06de\u06df\u0005\u0002"+
		"\u0000\u0000\u06df\u06e0\u0005\u00d7\u0000\u0000\u06e0\u06e1\u0003V+\u0000"+
		"\u06e1\u06e2\u0005\u0003\u0000\u0000\u06e2{\u0001\u0000\u0000\u0000\u06e3"+
		"\u06e4\u0005\u008c\u0000\u0000\u06e4\u06ef\u0005\u0002\u0000\u0000\u06e5"+
		"\u06e6\u0005\u008d\u0000\u0000\u06e6\u06e7\u0005\u0017\u0000\u0000\u06e7"+
		"\u06ec\u0003T*\u0000\u06e8\u06e9\u0005\u0004\u0000\u0000\u06e9\u06eb\u0003"+
		"T*\u0000\u06ea\u06e8\u0001\u0000\u0000\u0000\u06eb\u06ee\u0001\u0000\u0000"+
		"\u0000\u06ec\u06ea\u0001\u0000\u0000\u0000\u06ec\u06ed\u0001\u0000\u0000"+
		"\u0000\u06ed\u06f0\u0001\u0000\u0000\u0000\u06ee\u06ec\u0001\u0000\u0000"+
		"\u0000\u06ef\u06e5\u0001\u0000\u0000\u0000\u06ef\u06f0\u0001\u0000\u0000"+
		"\u0000\u06f0\u06fb\u0001\u0000\u0000\u0000\u06f1\u06f2\u0005\u0088\u0000"+
		"\u0000\u06f2\u06f3\u0005\u0017\u0000\u0000\u06f3\u06f8\u00034\u001a\u0000"+
		"\u06f4\u06f5\u0005\u0004\u0000\u0000\u06f5\u06f7\u00034\u001a\u0000\u06f6"+
		"\u06f4\u0001\u0000\u0000\u0000\u06f7\u06fa\u0001\u0000\u0000\u0000\u06f8"+
		"\u06f6\u0001\u0000\u0000\u0000\u06f8\u06f9\u0001\u0000\u0000\u0000\u06f9"+
		"\u06fc\u0001\u0000\u0000\u0000\u06fa\u06f8\u0001\u0000\u0000\u0000\u06fb"+
		"\u06f1\u0001\u0000\u0000\u0000\u06fb\u06fc\u0001\u0000\u0000\u0000\u06fc"+
		"\u06fe\u0001\u0000\u0000\u0000\u06fd\u06ff\u0003~?\u0000\u06fe\u06fd\u0001"+
		"\u0000\u0000\u0000\u06fe\u06ff\u0001\u0000\u0000\u0000\u06ff\u0700\u0001"+
		"\u0000\u0000\u0000\u0700\u0701\u0005\u0003\u0000\u0000\u0701}\u0001\u0000"+
		"\u0000\u0000\u0702\u0703\u0005\u0094\u0000\u0000\u0703\u071b\u0003\u0080"+
		"@\u0000\u0704\u0705\u0005\u00a7\u0000\u0000\u0705\u071b\u0003\u0080@\u0000"+
		"\u0706\u0707\u0005T\u0000\u0000\u0707\u071b\u0003\u0080@\u0000\u0708\u0709"+
		"\u0005\u0094\u0000\u0000\u0709\u070a\u0005\u0016\u0000\u0000\u070a\u070b"+
		"\u0003\u0080@\u0000\u070b\u070c\u0005\u000f\u0000\u0000\u070c\u070d\u0003"+
		"\u0080@\u0000\u070d\u071b\u0001\u0000\u0000\u0000\u070e\u070f\u0005\u00a7"+
		"\u0000\u0000\u070f\u0710\u0005\u0016\u0000\u0000\u0710\u0711\u0003\u0080"+
		"@\u0000\u0711\u0712\u0005\u000f\u0000\u0000\u0712\u0713\u0003\u0080@\u0000"+
		"\u0713\u071b\u0001\u0000\u0000\u0000\u0714\u0715\u0005T\u0000\u0000\u0715"+
		"\u0716\u0005\u0016\u0000\u0000\u0716\u0717\u0003\u0080@\u0000\u0717\u0718"+
		"\u0005\u000f\u0000\u0000\u0718\u0719\u0003\u0080@\u0000\u0719\u071b\u0001"+
		"\u0000\u0000\u0000\u071a\u0702\u0001\u0000\u0000\u0000\u071a\u0704\u0001"+
		"\u0000\u0000\u0000\u071a\u0706\u0001\u0000\u0000\u0000\u071a\u0708\u0001"+
		"\u0000\u0000\u0000\u071a\u070e\u0001\u0000\u0000\u0000\u071a\u0714\u0001"+
		"\u0000\u0000\u0000\u071b\u007f\u0001\u0000\u0000\u0000\u071c\u071d\u0005"+
		"\u00c9\u0000\u0000\u071d\u0726\u0005\u0090\u0000\u0000\u071e\u071f\u0005"+
		"\u00c9\u0000\u0000\u071f\u0726\u0005G\u0000\u0000\u0720\u0721\u0005\'"+
		"\u0000\u0000\u0721\u0726\u0005\u00a6\u0000\u0000\u0722\u0723\u0003T*\u0000"+
		"\u0723\u0724\u0007\u0012\u0000\u0000\u0724\u0726\u0001\u0000\u0000\u0000"+
		"\u0725\u071c\u0001\u0000\u0000\u0000\u0725\u071e\u0001\u0000\u0000\u0000"+
		"\u0725\u0720\u0001\u0000\u0000\u0000\u0725\u0722\u0001\u0000\u0000\u0000"+
		"\u0726\u0081\u0001\u0000\u0000\u0000\u0727\u0728\u0003\u0098L\u0000\u0728"+
		"\u0729\u0005\u00dd\u0000\u0000\u0729\u072a\u0003T*\u0000\u072a\u0083\u0001"+
		"\u0000\u0000\u0000\u072b\u072c\u0005I\u0000\u0000\u072c\u0730\u0007\u0013"+
		"\u0000\u0000\u072d\u072e\u0005\u00c7\u0000\u0000\u072e\u0730\u0007\u0014"+
		"\u0000\u0000\u072f\u072b\u0001\u0000\u0000\u0000\u072f\u072d\u0001\u0000"+
		"\u0000\u0000\u0730\u0085\u0001\u0000\u0000\u0000\u0731\u0732\u0005d\u0000"+
		"\u0000\u0732\u0733\u0005k\u0000\u0000\u0733\u0737\u0003\u0088D\u0000\u0734"+
		"\u0735\u0005\u0095\u0000\u0000\u0735\u0737\u0007\u0015\u0000\u0000\u0736"+
		"\u0731\u0001\u0000\u0000\u0000\u0736\u0734\u0001\u0000\u0000\u0000\u0737"+
		"\u0087\u0001\u0000\u0000\u0000\u0738\u0739\u0005\u0095\u0000\u0000\u0739"+
		"\u0740\u0005\u00ca\u0000\u0000\u073a\u073b\u0005\u0095\u0000\u0000\u073b"+
		"\u0740\u0005\"\u0000\u0000\u073c\u073d\u0005\u0099\u0000\u0000\u073d\u0740"+
		"\u0005\u0095\u0000\u0000\u073e\u0740\u0005\u00ad\u0000\u0000\u073f\u0738"+
		"\u0001\u0000\u0000\u0000\u073f\u073a\u0001\u0000\u0000\u0000\u073f\u073c"+
		"\u0001\u0000\u0000\u0000\u073f\u073e\u0001\u0000\u0000\u0000\u0740\u0089"+
		"\u0001\u0000\u0000\u0000\u0741\u0747\u0003T*\u0000\u0742\u0743\u0003\u0098"+
		"L\u0000\u0743\u0744\u0005\t\u0000\u0000\u0744\u0745\u0003T*\u0000\u0745"+
		"\u0747\u0001\u0000\u0000\u0000\u0746\u0741\u0001\u0000\u0000\u0000\u0746"+
		"\u0742\u0001\u0000\u0000\u0000\u0747\u008b\u0001\u0000\u0000\u0000\u0748"+
		"\u074d\u0005\u00ac\u0000\u0000\u0749\u074d\u00052\u0000\u0000\u074a\u074d"+
		"\u0005]\u0000\u0000\u074b\u074d\u0003\u0098L\u0000\u074c\u0748\u0001\u0000"+
		"\u0000\u0000\u074c\u0749\u0001\u0000\u0000\u0000\u074c\u074a\u0001\u0000"+
		"\u0000\u0000\u074c\u074b\u0001\u0000\u0000\u0000\u074d\u008d\u0001\u0000"+
		"\u0000\u0000\u074e\u0753\u0003\u0098L\u0000\u074f\u0750\u0005\u0001\u0000"+
		"\u0000\u0750\u0752\u0003\u0098L\u0000\u0751\u074f\u0001\u0000\u0000\u0000"+
		"\u0752\u0755\u0001\u0000\u0000\u0000\u0753\u0751\u0001\u0000\u0000\u0000"+
		"\u0753\u0754\u0001\u0000\u0000\u0000\u0754\u008f\u0001\u0000\u0000\u0000"+
		"\u0755\u0753\u0001\u0000\u0000\u0000\u0756\u0757\u0005H\u0000\u0000\u0757"+
		"\u0758\u0007\u0016\u0000\u0000\u0758\u0759\u0005\u0012\u0000\u0000\u0759"+
		"\u075a\u0005\u0082\u0000\u0000\u075a\u075b\u0003Z-\u0000\u075b\u0091\u0001"+
		"\u0000\u0000\u0000\u075c\u0760\u0005,\u0000\u0000\u075d\u0760\u0005)\u0000"+
		"\u0000\u075e\u0760\u0003\u0094J\u0000\u075f\u075c\u0001\u0000\u0000\u0000"+
		"\u075f\u075d\u0001\u0000\u0000\u0000\u075f\u075e\u0001\u0000\u0000\u0000"+
		"\u0760\u0093\u0001\u0000\u0000\u0000\u0761\u0762\u0005\u00cf\u0000\u0000"+
		"\u0762\u0767\u0003\u0098L\u0000\u0763\u0764\u0005\u00a2\u0000\u0000\u0764"+
		"\u0767\u0003\u0098L\u0000\u0765\u0767\u0003\u0098L\u0000\u0766\u0761\u0001"+
		"\u0000\u0000\u0000\u0766\u0763\u0001\u0000\u0000\u0000\u0766\u0765\u0001"+
		"\u0000\u0000\u0000\u0767\u0095\u0001\u0000\u0000\u0000\u0768\u076d\u0003"+
		"\u0098L\u0000\u0769\u076a\u0005\u0004\u0000\u0000\u076a\u076c\u0003\u0098"+
		"L\u0000\u076b\u0769\u0001\u0000\u0000\u0000\u076c\u076f\u0001\u0000\u0000"+
		"\u0000\u076d\u076b\u0001\u0000\u0000\u0000\u076d\u076e\u0001\u0000\u0000"+
		"\u0000\u076e\u0097\u0001\u0000\u0000\u0000\u076f\u076d\u0001\u0000\u0000"+
		"\u0000\u0770\u0776\u0005\u00ef\u0000\u0000\u0771\u0776\u0005\u00f1\u0000"+
		"\u0000\u0772\u0776\u0003\u009cN\u0000\u0773\u0776\u0005\u00f2\u0000\u0000"+
		"\u0774\u0776\u0005\u00f0\u0000\u0000\u0775\u0770\u0001\u0000\u0000\u0000"+
		"\u0775\u0771\u0001\u0000\u0000\u0000\u0775\u0772\u0001\u0000\u0000\u0000"+
		"\u0775\u0773\u0001\u0000\u0000\u0000\u0775\u0774\u0001\u0000\u0000\u0000"+
		"\u0776\u0099\u0001\u0000\u0000\u0000\u0777\u077b\u0005\u00ed\u0000\u0000"+
		"\u0778\u077b\u0005\u00ee\u0000\u0000\u0779\u077b\u0005\u00ec\u0000\u0000"+
		"\u077a\u0777\u0001\u0000\u0000\u0000\u077a\u0778\u0001\u0000\u0000\u0000"+
		"\u077a\u0779\u0001\u0000\u0000\u0000\u077b\u009b\u0001\u0000\u0000\u0000"+
		"\u077c\u077d\u0007\u0017\u0000\u0000\u077d\u009d\u0001\u0000\u0000\u0000"+
		"\u00f5\u00b4\u00b9\u00bf\u00c3\u00d1\u00d5\u00d9\u00dd\u00e5\u00e9\u00ec"+
		"\u00f3\u00fc\u0102\u0106\u010c\u0113\u011c\u0125\u0130\u0137\u0141\u0148"+
		"\u0150\u0158\u0160\u016c\u0172\u0177\u017d\u0186\u018f\u0194\u0198\u01a0"+
		"\u01a7\u01b4\u01b7\u01c1\u01c4\u01cb\u01d4\u01da\u01df\u01e3\u01ed\u01f0"+
		"\u01fa\u0207\u020d\u0212\u0218\u0221\u0227\u022e\u0236\u023b\u023f\u0247"+
		"\u024d\u0254\u0259\u025d\u0267\u026a\u026e\u0271\u0279\u027e\u0293\u0299"+
		"\u029f\u02a1\u02a7\u02ad\u02af\u02b7\u02b9\u02cc\u02d1\u02d8\u02e4\u02e6"+
		"\u02ee\u02f0\u0302\u0305\u0309\u030d\u031f\u0322\u0332\u0337\u0339\u033c"+
		"\u0342\u0349\u034e\u0354\u0358\u035c\u0362\u036a\u0379\u0380\u0385\u038c"+
		"\u0394\u0398\u039d\u03a8\u03b4\u03b7\u03bc\u03be\u03c7\u03c9\u03d1\u03d7"+
		"\u03da\u03dc\u03e8\u03ef\u03f3\u03f7\u03fb\u0402\u040b\u040e\u0412\u0417"+
		"\u041b\u041e\u0425\u0430\u0433\u043d\u0440\u044b\u0450\u0458\u045b\u045f"+
		"\u0463\u046e\u0471\u0478\u048b\u048f\u0493\u0497\u049b\u049f\u04a1\u04ac"+
		"\u04b1\u04ba\u04c0\u04c4\u04c6\u04ce\u04d5\u04e2\u04e8\u04f3\u04fa\u04fe"+
		"\u0506\u0508\u0515\u051d\u0526\u052c\u0534\u053a\u053e\u0543\u0548\u054e"+
		"\u055c\u055e\u057b\u0586\u0590\u0593\u0598\u059f\u05a2\u05ab\u05ae\u05b2"+
		"\u05b5\u05b8\u05c4\u05c7\u05da\u05de\u05e6\u05ea\u0603\u0606\u060f\u0615"+
		"\u061b\u0621\u062b\u0634\u064a\u064d\u0650\u065a\u065c\u0663\u0665\u066b"+
		"\u0673\u067d\u0683\u068f\u0692\u06ad\u06b9\u06be\u06c5\u06cb\u06d0\u06d6"+
		"\u06ec\u06ef\u06f8\u06fb\u06fe\u071a\u0725\u072f\u0736\u073f\u0746\u074c"+
		"\u0753\u075f\u0766\u076d\u0775\u077a";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}