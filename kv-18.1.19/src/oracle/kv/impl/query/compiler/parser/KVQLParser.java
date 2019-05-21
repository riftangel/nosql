// Generated from /home/markos/KVS/joins/kvstore/src/oracle/kv/impl/query/compiler/parser/KVQL.g4 by ANTLR 4.7.1
package oracle.kv.impl.query.compiler.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class KVQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, ACCOUNT=3, ADD=4, ADMIN=5, ALL=6, ALTER=7, ANCESTORS=8, 
		AND=9, AS=10, ASC=11, BY=12, CASE=13, CASCADE=14, CAST=15, COMMENT=16, 
		COUNT=17, CREATE=18, DAYS=19, DECLARE=20, DEFAULT=21, DESC=22, DESCENDANTS=23, 
		DESCRIBE=24, DROP=25, ELEMENTOF=26, ELSE=27, END=28, ES_SHARDS=29, ES_REPLICAS=30, 
		EXISTS=31, EXTRACT=32, FIRST=33, FORCE_INDEX=34, FORCE_PRIMARY_INDEX=35, 
		FROM=36, FULLTEXT=37, GRANT=38, GROUP=39, HOURS=40, IDENTIFIED=41, IF=42, 
		INDEX=43, INDEXES=44, IS=45, JSON=46, KEY=47, KEYOF=48, KEYS=49, LAST=50, 
		LIFETIME=51, LIMIT=52, LOCK=53, MINUTES=54, MODIFY=55, NESTED=56, NOT=57, 
		NULLS=58, OFFSET=59, OF=60, ON=61, ONLY=62, OR=63, ORDER=64, OVERRIDE=65, 
		PASSWORD=66, PREFER_INDEXES=67, PREFER_PRIMARY_INDEX=68, PRIMARY=69, PUT=70, 
		REMOVE=71, RETURNING=72, REVOKE=73, ROLE=74, ROLES=75, SECONDS=76, SELECT=77, 
		SEQ_TRANSFORM=78, SET=79, SHARD=80, SHOW=81, TABLE=82, TABLES=83, THEN=84, 
		TO=85, TTL=86, TYPE=87, UNLOCK=88, UPDATE=89, USER=90, USERS=91, USING=92, 
		VALUES=93, WHEN=94, WHERE=95, ALL_PRIVILEGES=96, IDENTIFIED_EXTERNALLY=97, 
		PASSWORD_EXPIRE=98, RETAIN_CURRENT_PASSWORD=99, CLEAR_RETAINED_PASSWORD=100, 
		ARRAY_T=101, BINARY_T=102, BOOLEAN_T=103, DOUBLE_T=104, ENUM_T=105, FLOAT_T=106, 
		INTEGER_T=107, LONG_T=108, MAP_T=109, NUMBER_T=110, RECORD_T=111, STRING_T=112, 
		TIMESTAMP_T=113, ANY_T=114, ANYATOMIC_T=115, ANYJSONATOMIC_T=116, ANYRECORD_T=117, 
		SCALAR_T=118, SEMI=119, COMMA=120, COLON=121, LP=122, RP=123, LBRACK=124, 
		RBRACK=125, LBRACE=126, RBRACE=127, STAR=128, DOT=129, DOLLAR=130, QUESTION_MARK=131, 
		LT=132, LTE=133, GT=134, GTE=135, EQ=136, NEQ=137, LT_ANY=138, LTE_ANY=139, 
		GT_ANY=140, GTE_ANY=141, EQ_ANY=142, NEQ_ANY=143, PLUS=144, MINUS=145, 
		DIV=146, NULL=147, FALSE=148, TRUE=149, INT=150, FLOAT=151, NUMBER=152, 
		DSTRING=153, STRING=154, SYSTEM_TABLE_NAME=155, ID=156, BAD_ID=157, WS=158, 
		C_COMMENT=159, LINE_COMMENT=160, LINE_COMMENT1=161, UnrecognizedToken=162;
	public static final int
		RULE_parse = 0, RULE_statement = 1, RULE_query = 2, RULE_prolog = 3, RULE_var_decl = 4, 
		RULE_var_name = 5, RULE_expr = 6, RULE_sfw_expr = 7, RULE_from_clause = 8, 
		RULE_nested_tables = 9, RULE_ancestor_tables = 10, RULE_descendant_tables = 11, 
		RULE_from_table = 12, RULE_aliased_table_name = 13, RULE_tab_alias = 14, 
		RULE_where_clause = 15, RULE_select_clause = 16, RULE_select_list = 17, 
		RULE_hints = 18, RULE_hint = 19, RULE_col_alias = 20, RULE_orderby_clause = 21, 
		RULE_sort_spec = 22, RULE_groupby_clause = 23, RULE_limit_clause = 24, 
		RULE_offset_clause = 25, RULE_or_expr = 26, RULE_and_expr = 27, RULE_not_expr = 28, 
		RULE_is_null_expr = 29, RULE_cond_expr = 30, RULE_exists_expr = 31, RULE_is_of_type_expr = 32, 
		RULE_comp_expr = 33, RULE_comp_op = 34, RULE_any_op = 35, RULE_add_expr = 36, 
		RULE_multiply_expr = 37, RULE_unary_expr = 38, RULE_path_expr = 39, RULE_map_step = 40, 
		RULE_map_field_step = 41, RULE_map_filter_step = 42, RULE_array_step = 43, 
		RULE_array_slice_step = 44, RULE_array_filter_step = 45, RULE_primary_expr = 46, 
		RULE_column_ref = 47, RULE_const_expr = 48, RULE_var_ref = 49, RULE_array_constructor = 50, 
		RULE_map_constructor = 51, RULE_transform_expr = 52, RULE_transform_input_expr = 53, 
		RULE_func_call = 54, RULE_count_star = 55, RULE_case_expr = 56, RULE_cast_expr = 57, 
		RULE_parenthesized_expr = 58, RULE_extract_expr = 59, RULE_update_statement = 60, 
		RULE_returning_clause = 61, RULE_update_clause = 62, RULE_set_clause = 63, 
		RULE_add_clause = 64, RULE_put_clause = 65, RULE_remove_clause = 66, RULE_ttl_clause = 67, 
		RULE_target_expr = 68, RULE_pos_expr = 69, RULE_quantified_type_def = 70, 
		RULE_type_def = 71, RULE_record_def = 72, RULE_field_def = 73, RULE_default_def = 74, 
		RULE_default_value = 75, RULE_not_null = 76, RULE_map_def = 77, RULE_array_def = 78, 
		RULE_integer_def = 79, RULE_json_def = 80, RULE_float_def = 81, RULE_string_def = 82, 
		RULE_enum_def = 83, RULE_boolean_def = 84, RULE_binary_def = 85, RULE_timestamp_def = 86, 
		RULE_any_def = 87, RULE_anyAtomic_def = 88, RULE_anyJsonAtomic_def = 89, 
		RULE_anyRecord_def = 90, RULE_id_path = 91, RULE_name_path = 92, RULE_field_name = 93, 
		RULE_create_table_statement = 94, RULE_table_name = 95, RULE_table_def = 96, 
		RULE_key_def = 97, RULE_shard_key_def = 98, RULE_id_list_with_size = 99, 
		RULE_id_with_size = 100, RULE_storage_size = 101, RULE_ttl_def = 102, 
		RULE_alter_table_statement = 103, RULE_alter_def = 104, RULE_alter_field_statements = 105, 
		RULE_add_field_statement = 106, RULE_drop_field_statement = 107, RULE_modify_field_statement = 108, 
		RULE_schema_path = 109, RULE_init_schema_path_step = 110, RULE_schema_path_step = 111, 
		RULE_drop_table_statement = 112, RULE_create_index_statement = 113, RULE_index_name = 114, 
		RULE_index_path_list = 115, RULE_index_path = 116, RULE_keys_expr = 117, 
		RULE_values_expr = 118, RULE_path_type = 119, RULE_create_text_index_statement = 120, 
		RULE_fts_field_list = 121, RULE_fts_path_list = 122, RULE_fts_path = 123, 
		RULE_es_properties = 124, RULE_es_property_assignment = 125, RULE_drop_index_statement = 126, 
		RULE_describe_statement = 127, RULE_schema_path_list = 128, RULE_show_statement = 129, 
		RULE_create_user_statement = 130, RULE_create_role_statement = 131, RULE_alter_user_statement = 132, 
		RULE_drop_user_statement = 133, RULE_drop_role_statement = 134, RULE_grant_statement = 135, 
		RULE_revoke_statement = 136, RULE_identifier_or_string = 137, RULE_identified_clause = 138, 
		RULE_create_user_identified_clause = 139, RULE_by_password = 140, RULE_password_lifetime = 141, 
		RULE_reset_password_clause = 142, RULE_account_lock = 143, RULE_grant_roles = 144, 
		RULE_grant_system_privileges = 145, RULE_grant_object_privileges = 146, 
		RULE_revoke_roles = 147, RULE_revoke_system_privileges = 148, RULE_revoke_object_privileges = 149, 
		RULE_principal = 150, RULE_sys_priv_list = 151, RULE_priv_item = 152, 
		RULE_obj_priv_list = 153, RULE_object = 154, RULE_json_text = 155, RULE_jsobject = 156, 
		RULE_jsarray = 157, RULE_jspair = 158, RULE_jsvalue = 159, RULE_comment = 160, 
		RULE_duration = 161, RULE_time_unit = 162, RULE_number = 163, RULE_string = 164, 
		RULE_id_list = 165, RULE_id = 166;
	public static final String[] ruleNames = {
		"parse", "statement", "query", "prolog", "var_decl", "var_name", "expr", 
		"sfw_expr", "from_clause", "nested_tables", "ancestor_tables", "descendant_tables", 
		"from_table", "aliased_table_name", "tab_alias", "where_clause", "select_clause", 
		"select_list", "hints", "hint", "col_alias", "orderby_clause", "sort_spec", 
		"groupby_clause", "limit_clause", "offset_clause", "or_expr", "and_expr", 
		"not_expr", "is_null_expr", "cond_expr", "exists_expr", "is_of_type_expr", 
		"comp_expr", "comp_op", "any_op", "add_expr", "multiply_expr", "unary_expr", 
		"path_expr", "map_step", "map_field_step", "map_filter_step", "array_step", 
		"array_slice_step", "array_filter_step", "primary_expr", "column_ref", 
		"const_expr", "var_ref", "array_constructor", "map_constructor", "transform_expr", 
		"transform_input_expr", "func_call", "count_star", "case_expr", "cast_expr", 
		"parenthesized_expr", "extract_expr", "update_statement", "returning_clause", 
		"update_clause", "set_clause", "add_clause", "put_clause", "remove_clause", 
		"ttl_clause", "target_expr", "pos_expr", "quantified_type_def", "type_def", 
		"record_def", "field_def", "default_def", "default_value", "not_null", 
		"map_def", "array_def", "integer_def", "json_def", "float_def", "string_def", 
		"enum_def", "boolean_def", "binary_def", "timestamp_def", "any_def", "anyAtomic_def", 
		"anyJsonAtomic_def", "anyRecord_def", "id_path", "name_path", "field_name", 
		"create_table_statement", "table_name", "table_def", "key_def", "shard_key_def", 
		"id_list_with_size", "id_with_size", "storage_size", "ttl_def", "alter_table_statement", 
		"alter_def", "alter_field_statements", "add_field_statement", "drop_field_statement", 
		"modify_field_statement", "schema_path", "init_schema_path_step", "schema_path_step", 
		"drop_table_statement", "create_index_statement", "index_name", "index_path_list", 
		"index_path", "keys_expr", "values_expr", "path_type", "create_text_index_statement", 
		"fts_field_list", "fts_path_list", "fts_path", "es_properties", "es_property_assignment", 
		"drop_index_statement", "describe_statement", "schema_path_list", "show_statement", 
		"create_user_statement", "create_role_statement", "alter_user_statement", 
		"drop_user_statement", "drop_role_statement", "grant_statement", "revoke_statement", 
		"identifier_or_string", "identified_clause", "create_user_identified_clause", 
		"by_password", "password_lifetime", "reset_password_clause", "account_lock", 
		"grant_roles", "grant_system_privileges", "grant_object_privileges", "revoke_roles", 
		"revoke_system_privileges", "revoke_object_privileges", "principal", "sys_priv_list", 
		"priv_item", "obj_priv_list", "object", "json_text", "jsobject", "jsarray", 
		"jspair", "jsvalue", "comment", "duration", "time_unit", "number", "string", 
		"id_list", "id"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'/*+'", "'*/'", null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, "'count'", null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, "'seq_transform'", null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, "';'", "','", "':'", "'('", "')'", "'['", "']'", "'{'", 
		"'}'", "'*'", "'.'", "'$'", "'?'", "'<'", "'<='", "'>'", "'>='", "'='", 
		"'!='", null, null, null, null, null, null, "'+'", "'-'", "'/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, "ACCOUNT", "ADD", "ADMIN", "ALL", "ALTER", "ANCESTORS", 
		"AND", "AS", "ASC", "BY", "CASE", "CASCADE", "CAST", "COMMENT", "COUNT", 
		"CREATE", "DAYS", "DECLARE", "DEFAULT", "DESC", "DESCENDANTS", "DESCRIBE", 
		"DROP", "ELEMENTOF", "ELSE", "END", "ES_SHARDS", "ES_REPLICAS", "EXISTS", 
		"EXTRACT", "FIRST", "FORCE_INDEX", "FORCE_PRIMARY_INDEX", "FROM", "FULLTEXT", 
		"GRANT", "GROUP", "HOURS", "IDENTIFIED", "IF", "INDEX", "INDEXES", "IS", 
		"JSON", "KEY", "KEYOF", "KEYS", "LAST", "LIFETIME", "LIMIT", "LOCK", "MINUTES", 
		"MODIFY", "NESTED", "NOT", "NULLS", "OFFSET", "OF", "ON", "ONLY", "OR", 
		"ORDER", "OVERRIDE", "PASSWORD", "PREFER_INDEXES", "PREFER_PRIMARY_INDEX", 
		"PRIMARY", "PUT", "REMOVE", "RETURNING", "REVOKE", "ROLE", "ROLES", "SECONDS", 
		"SELECT", "SEQ_TRANSFORM", "SET", "SHARD", "SHOW", "TABLE", "TABLES", 
		"THEN", "TO", "TTL", "TYPE", "UNLOCK", "UPDATE", "USER", "USERS", "USING", 
		"VALUES", "WHEN", "WHERE", "ALL_PRIVILEGES", "IDENTIFIED_EXTERNALLY", 
		"PASSWORD_EXPIRE", "RETAIN_CURRENT_PASSWORD", "CLEAR_RETAINED_PASSWORD", 
		"ARRAY_T", "BINARY_T", "BOOLEAN_T", "DOUBLE_T", "ENUM_T", "FLOAT_T", "INTEGER_T", 
		"LONG_T", "MAP_T", "NUMBER_T", "RECORD_T", "STRING_T", "TIMESTAMP_T", 
		"ANY_T", "ANYATOMIC_T", "ANYJSONATOMIC_T", "ANYRECORD_T", "SCALAR_T", 
		"SEMI", "COMMA", "COLON", "LP", "RP", "LBRACK", "RBRACK", "LBRACE", "RBRACE", 
		"STAR", "DOT", "DOLLAR", "QUESTION_MARK", "LT", "LTE", "GT", "GTE", "EQ", 
		"NEQ", "LT_ANY", "LTE_ANY", "GT_ANY", "GTE_ANY", "EQ_ANY", "NEQ_ANY", 
		"PLUS", "MINUS", "DIV", "NULL", "FALSE", "TRUE", "INT", "FLOAT", "NUMBER", 
		"DSTRING", "STRING", "SYSTEM_TABLE_NAME", "ID", "BAD_ID", "WS", "C_COMMENT", 
		"LINE_COMMENT", "LINE_COMMENT1", "UnrecognizedToken"
	};
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
	public String getGrammarFileName() { return "KVQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public KVQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ParseContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(KVQLParser.EOF, 0); }
		public ParseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterParse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitParse(this);
		}
	}

	public final ParseContext parse() throws RecognitionException {
		ParseContext _localctx = new ParseContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_parse);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(334);
			statement();
			setState(335);
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

	public static class StatementContext extends ParserRuleContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public Update_statementContext update_statement() {
			return getRuleContext(Update_statementContext.class,0);
		}
		public Create_table_statementContext create_table_statement() {
			return getRuleContext(Create_table_statementContext.class,0);
		}
		public Create_index_statementContext create_index_statement() {
			return getRuleContext(Create_index_statementContext.class,0);
		}
		public Create_user_statementContext create_user_statement() {
			return getRuleContext(Create_user_statementContext.class,0);
		}
		public Create_role_statementContext create_role_statement() {
			return getRuleContext(Create_role_statementContext.class,0);
		}
		public Drop_index_statementContext drop_index_statement() {
			return getRuleContext(Drop_index_statementContext.class,0);
		}
		public Create_text_index_statementContext create_text_index_statement() {
			return getRuleContext(Create_text_index_statementContext.class,0);
		}
		public Drop_role_statementContext drop_role_statement() {
			return getRuleContext(Drop_role_statementContext.class,0);
		}
		public Drop_user_statementContext drop_user_statement() {
			return getRuleContext(Drop_user_statementContext.class,0);
		}
		public Alter_table_statementContext alter_table_statement() {
			return getRuleContext(Alter_table_statementContext.class,0);
		}
		public Alter_user_statementContext alter_user_statement() {
			return getRuleContext(Alter_user_statementContext.class,0);
		}
		public Drop_table_statementContext drop_table_statement() {
			return getRuleContext(Drop_table_statementContext.class,0);
		}
		public Grant_statementContext grant_statement() {
			return getRuleContext(Grant_statementContext.class,0);
		}
		public Revoke_statementContext revoke_statement() {
			return getRuleContext(Revoke_statementContext.class,0);
		}
		public Describe_statementContext describe_statement() {
			return getRuleContext(Describe_statementContext.class,0);
		}
		public Show_statementContext show_statement() {
			return getRuleContext(Show_statementContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitStatement(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(354);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				{
				setState(337);
				query();
				}
				break;
			case 2:
				{
				setState(338);
				update_statement();
				}
				break;
			case 3:
				{
				setState(339);
				create_table_statement();
				}
				break;
			case 4:
				{
				setState(340);
				create_index_statement();
				}
				break;
			case 5:
				{
				setState(341);
				create_user_statement();
				}
				break;
			case 6:
				{
				setState(342);
				create_role_statement();
				}
				break;
			case 7:
				{
				setState(343);
				drop_index_statement();
				}
				break;
			case 8:
				{
				setState(344);
				create_text_index_statement();
				}
				break;
			case 9:
				{
				setState(345);
				drop_role_statement();
				}
				break;
			case 10:
				{
				setState(346);
				drop_user_statement();
				}
				break;
			case 11:
				{
				setState(347);
				alter_table_statement();
				}
				break;
			case 12:
				{
				setState(348);
				alter_user_statement();
				}
				break;
			case 13:
				{
				setState(349);
				drop_table_statement();
				}
				break;
			case 14:
				{
				setState(350);
				grant_statement();
				}
				break;
			case 15:
				{
				setState(351);
				revoke_statement();
				}
				break;
			case 16:
				{
				setState(352);
				describe_statement();
				}
				break;
			case 17:
				{
				setState(353);
				show_statement();
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

	public static class QueryContext extends ParserRuleContext {
		public Sfw_exprContext sfw_expr() {
			return getRuleContext(Sfw_exprContext.class,0);
		}
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitQuery(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(357);
			_la = _input.LA(1);
			if (_la==DECLARE) {
				{
				setState(356);
				prolog();
				}
			}

			setState(359);
			sfw_expr();
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

	public static class PrologContext extends ParserRuleContext {
		public TerminalNode DECLARE() { return getToken(KVQLParser.DECLARE, 0); }
		public List<Var_declContext> var_decl() {
			return getRuleContexts(Var_declContext.class);
		}
		public Var_declContext var_decl(int i) {
			return getRuleContext(Var_declContext.class,i);
		}
		public List<TerminalNode> SEMI() { return getTokens(KVQLParser.SEMI); }
		public TerminalNode SEMI(int i) {
			return getToken(KVQLParser.SEMI, i);
		}
		public PrologContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prolog; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterProlog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitProlog(this);
		}
	}

	public final PrologContext prolog() throws RecognitionException {
		PrologContext _localctx = new PrologContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_prolog);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(361);
			match(DECLARE);
			setState(362);
			var_decl();
			setState(363);
			match(SEMI);
			setState(369);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOLLAR) {
				{
				{
				setState(364);
				var_decl();
				setState(365);
				match(SEMI);
				}
				}
				setState(371);
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

	public static class Var_declContext extends ParserRuleContext {
		public Var_nameContext var_name() {
			return getRuleContext(Var_nameContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Var_declContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_var_decl; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterVar_decl(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitVar_decl(this);
		}
	}

	public final Var_declContext var_decl() throws RecognitionException {
		Var_declContext _localctx = new Var_declContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_var_decl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(372);
			var_name();
			setState(373);
			type_def();
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

	public static class Var_nameContext extends ParserRuleContext {
		public TerminalNode DOLLAR() { return getToken(KVQLParser.DOLLAR, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Var_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_var_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterVar_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitVar_name(this);
		}
	}

	public final Var_nameContext var_name() throws RecognitionException {
		Var_nameContext _localctx = new Var_nameContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_var_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(375);
			match(DOLLAR);
			setState(376);
			id();
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

	public static class ExprContext extends ParserRuleContext {
		public Or_exprContext or_expr() {
			return getRuleContext(Or_exprContext.class,0);
		}
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitExpr(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		ExprContext _localctx = new ExprContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(378);
			or_expr(0);
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

	public static class Sfw_exprContext extends ParserRuleContext {
		public Select_clauseContext select_clause() {
			return getRuleContext(Select_clauseContext.class,0);
		}
		public From_clauseContext from_clause() {
			return getRuleContext(From_clauseContext.class,0);
		}
		public Where_clauseContext where_clause() {
			return getRuleContext(Where_clauseContext.class,0);
		}
		public Groupby_clauseContext groupby_clause() {
			return getRuleContext(Groupby_clauseContext.class,0);
		}
		public Orderby_clauseContext orderby_clause() {
			return getRuleContext(Orderby_clauseContext.class,0);
		}
		public Limit_clauseContext limit_clause() {
			return getRuleContext(Limit_clauseContext.class,0);
		}
		public Offset_clauseContext offset_clause() {
			return getRuleContext(Offset_clauseContext.class,0);
		}
		public Sfw_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sfw_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSfw_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSfw_expr(this);
		}
	}

	public final Sfw_exprContext sfw_expr() throws RecognitionException {
		Sfw_exprContext _localctx = new Sfw_exprContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_sfw_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(380);
			select_clause();
			setState(381);
			from_clause();
			setState(383);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(382);
				where_clause();
				}
			}

			setState(386);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(385);
				groupby_clause();
				}
			}

			setState(389);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(388);
				orderby_clause();
				}
			}

			setState(392);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(391);
				limit_clause();
				}
			}

			setState(395);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(394);
				offset_clause();
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

	public static class From_clauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public From_tableContext from_table() {
			return getRuleContext(From_tableContext.class,0);
		}
		public Nested_tablesContext nested_tables() {
			return getRuleContext(Nested_tablesContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Var_nameContext> var_name() {
			return getRuleContexts(Var_nameContext.class);
		}
		public Var_nameContext var_name(int i) {
			return getRuleContext(Var_nameContext.class,i);
		}
		public List<TerminalNode> AS() { return getTokens(KVQLParser.AS); }
		public TerminalNode AS(int i) {
			return getToken(KVQLParser.AS, i);
		}
		public From_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_from_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFrom_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFrom_clause(this);
		}
	}

	public final From_clauseContext from_clause() throws RecognitionException {
		From_clauseContext _localctx = new From_clauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_from_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(397);
			match(FROM);
			setState(400);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				{
				setState(398);
				from_table();
				}
				break;
			case 2:
				{
				setState(399);
				nested_tables();
				}
				break;
			}
			setState(411);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(402);
				match(COMMA);
				setState(403);
				expr();
				{
				setState(405);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(404);
					match(AS);
					}
				}

				setState(407);
				var_name();
				}
				}
				}
				setState(413);
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

	public static class Nested_tablesContext extends ParserRuleContext {
		public TerminalNode NESTED() { return getToken(KVQLParser.NESTED, 0); }
		public TerminalNode TABLES() { return getToken(KVQLParser.TABLES, 0); }
		public List<TerminalNode> LP() { return getTokens(KVQLParser.LP); }
		public TerminalNode LP(int i) {
			return getToken(KVQLParser.LP, i);
		}
		public From_tableContext from_table() {
			return getRuleContext(From_tableContext.class,0);
		}
		public List<TerminalNode> RP() { return getTokens(KVQLParser.RP); }
		public TerminalNode RP(int i) {
			return getToken(KVQLParser.RP, i);
		}
		public TerminalNode ANCESTORS() { return getToken(KVQLParser.ANCESTORS, 0); }
		public Ancestor_tablesContext ancestor_tables() {
			return getRuleContext(Ancestor_tablesContext.class,0);
		}
		public TerminalNode DESCENDANTS() { return getToken(KVQLParser.DESCENDANTS, 0); }
		public Descendant_tablesContext descendant_tables() {
			return getRuleContext(Descendant_tablesContext.class,0);
		}
		public Nested_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nested_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNested_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNested_tables(this);
		}
	}

	public final Nested_tablesContext nested_tables() throws RecognitionException {
		Nested_tablesContext _localctx = new Nested_tablesContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_nested_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(414);
			match(NESTED);
			setState(415);
			match(TABLES);
			setState(416);
			match(LP);
			setState(417);
			from_table();
			setState(423);
			_la = _input.LA(1);
			if (_la==ANCESTORS) {
				{
				setState(418);
				match(ANCESTORS);
				setState(419);
				match(LP);
				setState(420);
				ancestor_tables();
				setState(421);
				match(RP);
				}
			}

			setState(430);
			_la = _input.LA(1);
			if (_la==DESCENDANTS) {
				{
				setState(425);
				match(DESCENDANTS);
				setState(426);
				match(LP);
				setState(427);
				descendant_tables();
				setState(428);
				match(RP);
				}
			}

			setState(432);
			match(RP);
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

	public static class Ancestor_tablesContext extends ParserRuleContext {
		public List<From_tableContext> from_table() {
			return getRuleContexts(From_tableContext.class);
		}
		public From_tableContext from_table(int i) {
			return getRuleContext(From_tableContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Ancestor_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ancestor_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAncestor_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAncestor_tables(this);
		}
	}

	public final Ancestor_tablesContext ancestor_tables() throws RecognitionException {
		Ancestor_tablesContext _localctx = new Ancestor_tablesContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_ancestor_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(434);
			from_table();
			setState(439);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(435);
				match(COMMA);
				setState(436);
				from_table();
				}
				}
				setState(441);
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

	public static class Descendant_tablesContext extends ParserRuleContext {
		public List<From_tableContext> from_table() {
			return getRuleContexts(From_tableContext.class);
		}
		public From_tableContext from_table(int i) {
			return getRuleContext(From_tableContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Descendant_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descendant_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDescendant_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDescendant_tables(this);
		}
	}

	public final Descendant_tablesContext descendant_tables() throws RecognitionException {
		Descendant_tablesContext _localctx = new Descendant_tablesContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_descendant_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(442);
			from_table();
			setState(447);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(443);
				match(COMMA);
				setState(444);
				from_table();
				}
				}
				setState(449);
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

	public static class From_tableContext extends ParserRuleContext {
		public Aliased_table_nameContext aliased_table_name() {
			return getRuleContext(Aliased_table_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Or_exprContext or_expr() {
			return getRuleContext(Or_exprContext.class,0);
		}
		public From_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_from_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFrom_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFrom_table(this);
		}
	}

	public final From_tableContext from_table() throws RecognitionException {
		From_tableContext _localctx = new From_tableContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_from_table);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(450);
			aliased_table_name();
			setState(453);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(451);
				match(ON);
				setState(452);
				or_expr(0);
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

	public static class Aliased_table_nameContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode SYSTEM_TABLE_NAME() { return getToken(KVQLParser.SYSTEM_TABLE_NAME, 0); }
		public Tab_aliasContext tab_alias() {
			return getRuleContext(Tab_aliasContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Aliased_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliased_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAliased_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAliased_table_name(this);
		}
	}

	public final Aliased_table_nameContext aliased_table_name() throws RecognitionException {
		Aliased_table_nameContext _localctx = new Aliased_table_nameContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_aliased_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(457);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
			case BAD_ID:
				{
				setState(455);
				table_name();
				}
				break;
			case SYSTEM_TABLE_NAME:
				{
				setState(456);
				match(SYSTEM_TABLE_NAME);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(463);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
			case 1:
				{
				setState(460);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
				case 1:
					{
					setState(459);
					match(AS);
					}
					break;
				}
				setState(462);
				tab_alias();
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

	public static class Tab_aliasContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode DOLLAR() { return getToken(KVQLParser.DOLLAR, 0); }
		public Tab_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tab_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTab_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTab_alias(this);
		}
	}

	public final Tab_aliasContext tab_alias() throws RecognitionException {
		Tab_aliasContext _localctx = new Tab_aliasContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_tab_alias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(466);
			_la = _input.LA(1);
			if (_la==DOLLAR) {
				{
				setState(465);
				match(DOLLAR);
				}
			}

			setState(468);
			id();
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

	public static class Where_clauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Where_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_where_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterWhere_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitWhere_clause(this);
		}
	}

	public final Where_clauseContext where_clause() throws RecognitionException {
		Where_clauseContext _localctx = new Where_clauseContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_where_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(470);
			match(WHERE);
			setState(471);
			expr();
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

	public static class Select_clauseContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(KVQLParser.SELECT, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public Select_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSelect_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSelect_clause(this);
		}
	}

	public final Select_clauseContext select_clause() throws RecognitionException {
		Select_clauseContext _localctx = new Select_clauseContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_select_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(473);
			match(SELECT);
			setState(474);
			select_list();
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

	public static class Select_listContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(KVQLParser.STAR, 0); }
		public HintsContext hints() {
			return getRuleContext(HintsContext.class,0);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Col_aliasContext> col_alias() {
			return getRuleContexts(Col_aliasContext.class);
		}
		public Col_aliasContext col_alias(int i) {
			return getRuleContext(Col_aliasContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Select_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSelect_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSelect_list(this);
		}
	}

	public final Select_listContext select_list() throws RecognitionException {
		Select_listContext _localctx = new Select_listContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_select_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(477);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(476);
				hints();
				}
			}

			setState(491);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(479);
				match(STAR);
				}
				break;
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case LP:
			case LBRACK:
			case LBRACE:
			case DOLLAR:
			case PLUS:
			case MINUS:
			case NULL:
			case FALSE:
			case TRUE:
			case INT:
			case FLOAT:
			case NUMBER:
			case DSTRING:
			case STRING:
			case ID:
			case BAD_ID:
				{
				{
				setState(480);
				expr();
				setState(481);
				col_alias();
				setState(488);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(482);
					match(COMMA);
					setState(483);
					expr();
					setState(484);
					col_alias();
					}
					}
					setState(490);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class HintsContext extends ParserRuleContext {
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public HintsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hints; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterHints(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitHints(this);
		}
	}

	public final HintsContext hints() throws RecognitionException {
		HintsContext _localctx = new HintsContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_hints);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(493);
			match(T__0);
			setState(497);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 34)) & ~0x3f) == 0 && ((1L << (_la - 34)) & ((1L << (FORCE_INDEX - 34)) | (1L << (FORCE_PRIMARY_INDEX - 34)) | (1L << (PREFER_INDEXES - 34)) | (1L << (PREFER_PRIMARY_INDEX - 34)))) != 0)) {
				{
				{
				setState(494);
				hint();
				}
				}
				setState(499);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(500);
			match(T__1);
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

	public static class HintContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(KVQLParser.STRING, 0); }
		public TerminalNode PREFER_INDEXES() { return getToken(KVQLParser.PREFER_INDEXES, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode FORCE_INDEX() { return getToken(KVQLParser.FORCE_INDEX, 0); }
		public List<Index_nameContext> index_name() {
			return getRuleContexts(Index_nameContext.class);
		}
		public Index_nameContext index_name(int i) {
			return getRuleContext(Index_nameContext.class,i);
		}
		public TerminalNode PREFER_PRIMARY_INDEX() { return getToken(KVQLParser.PREFER_PRIMARY_INDEX, 0); }
		public TerminalNode FORCE_PRIMARY_INDEX() { return getToken(KVQLParser.FORCE_PRIMARY_INDEX, 0); }
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitHint(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_hint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(529);
			switch (_input.LA(1)) {
			case PREFER_INDEXES:
				{
				{
				setState(502);
				match(PREFER_INDEXES);
				setState(503);
				match(LP);
				setState(504);
				table_name();
				setState(508);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)))) != 0) || _la==ID || _la==BAD_ID) {
					{
					{
					setState(505);
					index_name();
					}
					}
					setState(510);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(511);
				match(RP);
				}
				}
				break;
			case FORCE_INDEX:
				{
				{
				setState(513);
				match(FORCE_INDEX);
				setState(514);
				match(LP);
				setState(515);
				table_name();
				setState(516);
				index_name();
				setState(517);
				match(RP);
				}
				}
				break;
			case PREFER_PRIMARY_INDEX:
				{
				{
				setState(519);
				match(PREFER_PRIMARY_INDEX);
				setState(520);
				match(LP);
				setState(521);
				table_name();
				setState(522);
				match(RP);
				}
				}
				break;
			case FORCE_PRIMARY_INDEX:
				{
				{
				setState(524);
				match(FORCE_PRIMARY_INDEX);
				setState(525);
				match(LP);
				setState(526);
				table_name();
				setState(527);
				match(RP);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(532);
			_la = _input.LA(1);
			if (_la==STRING) {
				{
				setState(531);
				match(STRING);
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

	public static class Col_aliasContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Col_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_col_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCol_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCol_alias(this);
		}
	}

	public final Col_aliasContext col_alias() throws RecognitionException {
		Col_aliasContext _localctx = new Col_aliasContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_col_alias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(536);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(534);
				match(AS);
				setState(535);
				id();
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

	public static class Orderby_clauseContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(KVQLParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Sort_specContext> sort_spec() {
			return getRuleContexts(Sort_specContext.class);
		}
		public Sort_specContext sort_spec(int i) {
			return getRuleContext(Sort_specContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Orderby_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderby_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOrderby_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOrderby_clause(this);
		}
	}

	public final Orderby_clauseContext orderby_clause() throws RecognitionException {
		Orderby_clauseContext _localctx = new Orderby_clauseContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_orderby_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(538);
			match(ORDER);
			setState(539);
			match(BY);
			setState(540);
			expr();
			setState(541);
			sort_spec();
			setState(548);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(542);
				match(COMMA);
				setState(543);
				expr();
				setState(544);
				sort_spec();
				}
				}
				setState(550);
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

	public static class Sort_specContext extends ParserRuleContext {
		public TerminalNode NULLS() { return getToken(KVQLParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(KVQLParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(KVQLParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(KVQLParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(KVQLParser.LAST, 0); }
		public Sort_specContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sort_spec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSort_spec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSort_spec(this);
		}
	}

	public final Sort_specContext sort_spec() throws RecognitionException {
		Sort_specContext _localctx = new Sort_specContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_sort_spec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(552);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(551);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(556);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(554);
				match(NULLS);
				setState(555);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
				_errHandler.recoverInline(this);
				} else {
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

	public static class Groupby_clauseContext extends ParserRuleContext {
		public TerminalNode GROUP() { return getToken(KVQLParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Groupby_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupby_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGroupby_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGroupby_clause(this);
		}
	}

	public final Groupby_clauseContext groupby_clause() throws RecognitionException {
		Groupby_clauseContext _localctx = new Groupby_clauseContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_groupby_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(558);
			match(GROUP);
			setState(559);
			match(BY);
			setState(560);
			expr();
			setState(565);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(561);
				match(COMMA);
				setState(562);
				expr();
				}
				}
				setState(567);
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

	public static class Limit_clauseContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(KVQLParser.LIMIT, 0); }
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Limit_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limit_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterLimit_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitLimit_clause(this);
		}
	}

	public final Limit_clauseContext limit_clause() throws RecognitionException {
		Limit_clauseContext _localctx = new Limit_clauseContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_limit_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568);
			match(LIMIT);
			setState(569);
			add_expr();
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

	public static class Offset_clauseContext extends ParserRuleContext {
		public TerminalNode OFFSET() { return getToken(KVQLParser.OFFSET, 0); }
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Offset_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offset_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOffset_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOffset_clause(this);
		}
	}

	public final Offset_clauseContext offset_clause() throws RecognitionException {
		Offset_clauseContext _localctx = new Offset_clauseContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_offset_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(571);
			match(OFFSET);
			setState(572);
			add_expr();
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

	public static class Or_exprContext extends ParserRuleContext {
		public And_exprContext and_expr() {
			return getRuleContext(And_exprContext.class,0);
		}
		public Or_exprContext or_expr() {
			return getRuleContext(Or_exprContext.class,0);
		}
		public TerminalNode OR() { return getToken(KVQLParser.OR, 0); }
		public Or_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_or_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOr_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOr_expr(this);
		}
	}

	public final Or_exprContext or_expr() throws RecognitionException {
		return or_expr(0);
	}

	private Or_exprContext or_expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		Or_exprContext _localctx = new Or_exprContext(_ctx, _parentState);
		Or_exprContext _prevctx = _localctx;
		int _startState = 52;
		enterRecursionRule(_localctx, 52, RULE_or_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(575);
			and_expr(0);
			}
			_ctx.stop = _input.LT(-1);
			setState(582);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,32,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new Or_exprContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_or_expr);
					setState(577);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(578);
					match(OR);
					setState(579);
					and_expr(0);
					}
					} 
				}
				setState(584);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,32,_ctx);
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

	public static class And_exprContext extends ParserRuleContext {
		public Not_exprContext not_expr() {
			return getRuleContext(Not_exprContext.class,0);
		}
		public And_exprContext and_expr() {
			return getRuleContext(And_exprContext.class,0);
		}
		public TerminalNode AND() { return getToken(KVQLParser.AND, 0); }
		public And_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_and_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnd_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnd_expr(this);
		}
	}

	public final And_exprContext and_expr() throws RecognitionException {
		return and_expr(0);
	}

	private And_exprContext and_expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		And_exprContext _localctx = new And_exprContext(_ctx, _parentState);
		And_exprContext _prevctx = _localctx;
		int _startState = 54;
		enterRecursionRule(_localctx, 54, RULE_and_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(586);
			not_expr();
			}
			_ctx.stop = _input.LT(-1);
			setState(593);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,33,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new And_exprContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_and_expr);
					setState(588);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(589);
					match(AND);
					setState(590);
					not_expr();
					}
					} 
				}
				setState(595);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,33,_ctx);
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

	public static class Not_exprContext extends ParserRuleContext {
		public Is_null_exprContext is_null_expr() {
			return getRuleContext(Is_null_exprContext.class,0);
		}
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public Not_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNot_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNot_expr(this);
		}
	}

	public final Not_exprContext not_expr() throws RecognitionException {
		Not_exprContext _localctx = new Not_exprContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_not_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(597);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
			case 1:
				{
				setState(596);
				match(NOT);
				}
				break;
			}
			setState(599);
			is_null_expr();
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

	public static class Is_null_exprContext extends ParserRuleContext {
		public Cond_exprContext cond_expr() {
			return getRuleContext(Cond_exprContext.class,0);
		}
		public TerminalNode IS() { return getToken(KVQLParser.IS, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public Is_null_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_is_null_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIs_null_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIs_null_expr(this);
		}
	}

	public final Is_null_exprContext is_null_expr() throws RecognitionException {
		Is_null_exprContext _localctx = new Is_null_exprContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_is_null_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(601);
			cond_expr();
			setState(607);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
			case 1:
				{
				setState(602);
				match(IS);
				setState(604);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(603);
					match(NOT);
					}
				}

				setState(606);
				match(NULL);
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

	public static class Cond_exprContext extends ParserRuleContext {
		public Comp_exprContext comp_expr() {
			return getRuleContext(Comp_exprContext.class,0);
		}
		public Exists_exprContext exists_expr() {
			return getRuleContext(Exists_exprContext.class,0);
		}
		public Is_of_type_exprContext is_of_type_expr() {
			return getRuleContext(Is_of_type_exprContext.class,0);
		}
		public Cond_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cond_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCond_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCond_expr(this);
		}
	}

	public final Cond_exprContext cond_expr() throws RecognitionException {
		Cond_exprContext _localctx = new Cond_exprContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_cond_expr);
		try {
			setState(612);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(609);
				comp_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(610);
				exists_expr();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(611);
				is_of_type_expr();
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

	public static class Exists_exprContext extends ParserRuleContext {
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Exists_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exists_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterExists_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitExists_expr(this);
		}
	}

	public final Exists_exprContext exists_expr() throws RecognitionException {
		Exists_exprContext _localctx = new Exists_exprContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_exists_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(614);
			match(EXISTS);
			setState(615);
			add_expr();
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

	public static class Is_of_type_exprContext extends ParserRuleContext {
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public TerminalNode IS() { return getToken(KVQLParser.IS, 0); }
		public TerminalNode OF() { return getToken(KVQLParser.OF, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Quantified_type_defContext> quantified_type_def() {
			return getRuleContexts(Quantified_type_defContext.class);
		}
		public Quantified_type_defContext quantified_type_def(int i) {
			return getRuleContext(Quantified_type_defContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode TYPE() { return getToken(KVQLParser.TYPE, 0); }
		public List<TerminalNode> ONLY() { return getTokens(KVQLParser.ONLY); }
		public TerminalNode ONLY(int i) {
			return getToken(KVQLParser.ONLY, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Is_of_type_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_is_of_type_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIs_of_type_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIs_of_type_expr(this);
		}
	}

	public final Is_of_type_exprContext is_of_type_expr() throws RecognitionException {
		Is_of_type_exprContext _localctx = new Is_of_type_exprContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_is_of_type_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(617);
			add_expr();
			setState(618);
			match(IS);
			setState(620);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(619);
				match(NOT);
				}
			}

			setState(622);
			match(OF);
			setState(624);
			_la = _input.LA(1);
			if (_la==TYPE) {
				{
				setState(623);
				match(TYPE);
				}
			}

			setState(626);
			match(LP);
			setState(628);
			_la = _input.LA(1);
			if (_la==ONLY) {
				{
				setState(627);
				match(ONLY);
				}
			}

			setState(630);
			quantified_type_def();
			setState(638);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(631);
				match(COMMA);
				setState(633);
				_la = _input.LA(1);
				if (_la==ONLY) {
					{
					setState(632);
					match(ONLY);
					}
				}

				setState(635);
				quantified_type_def();
				}
				}
				setState(640);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(641);
			match(RP);
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

	public static class Comp_exprContext extends ParserRuleContext {
		public List<Add_exprContext> add_expr() {
			return getRuleContexts(Add_exprContext.class);
		}
		public Add_exprContext add_expr(int i) {
			return getRuleContext(Add_exprContext.class,i);
		}
		public Comp_opContext comp_op() {
			return getRuleContext(Comp_opContext.class,0);
		}
		public Any_opContext any_op() {
			return getRuleContext(Any_opContext.class,0);
		}
		public Comp_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterComp_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitComp_expr(this);
		}
	}

	public final Comp_exprContext comp_expr() throws RecognitionException {
		Comp_exprContext _localctx = new Comp_exprContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_comp_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(643);
			add_expr();
			setState(650);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
			case 1:
				{
				setState(646);
				switch (_input.LA(1)) {
				case LT:
				case LTE:
				case GT:
				case GTE:
				case EQ:
				case NEQ:
					{
					setState(644);
					comp_op();
					}
					break;
				case LT_ANY:
				case LTE_ANY:
				case GT_ANY:
				case GTE_ANY:
				case EQ_ANY:
				case NEQ_ANY:
					{
					setState(645);
					any_op();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(648);
				add_expr();
				}
				break;
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

	public static class Comp_opContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(KVQLParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(KVQLParser.NEQ, 0); }
		public TerminalNode GT() { return getToken(KVQLParser.GT, 0); }
		public TerminalNode GTE() { return getToken(KVQLParser.GTE, 0); }
		public TerminalNode LT() { return getToken(KVQLParser.LT, 0); }
		public TerminalNode LTE() { return getToken(KVQLParser.LTE, 0); }
		public Comp_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_op; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterComp_op(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitComp_op(this);
		}
	}

	public final Comp_opContext comp_op() throws RecognitionException {
		Comp_opContext _localctx = new Comp_opContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_comp_op);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(652);
			_la = _input.LA(1);
			if ( !(((((_la - 132)) & ~0x3f) == 0 && ((1L << (_la - 132)) & ((1L << (LT - 132)) | (1L << (LTE - 132)) | (1L << (GT - 132)) | (1L << (GTE - 132)) | (1L << (EQ - 132)) | (1L << (NEQ - 132)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class Any_opContext extends ParserRuleContext {
		public TerminalNode EQ_ANY() { return getToken(KVQLParser.EQ_ANY, 0); }
		public TerminalNode NEQ_ANY() { return getToken(KVQLParser.NEQ_ANY, 0); }
		public TerminalNode GT_ANY() { return getToken(KVQLParser.GT_ANY, 0); }
		public TerminalNode GTE_ANY() { return getToken(KVQLParser.GTE_ANY, 0); }
		public TerminalNode LT_ANY() { return getToken(KVQLParser.LT_ANY, 0); }
		public TerminalNode LTE_ANY() { return getToken(KVQLParser.LTE_ANY, 0); }
		public Any_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any_op; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAny_op(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAny_op(this);
		}
	}

	public final Any_opContext any_op() throws RecognitionException {
		Any_opContext _localctx = new Any_opContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_any_op);
		try {
			setState(660);
			switch (_input.LA(1)) {
			case EQ_ANY:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(654);
				match(EQ_ANY);
				}
				}
				break;
			case NEQ_ANY:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(655);
				match(NEQ_ANY);
				}
				}
				break;
			case GT_ANY:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(656);
				match(GT_ANY);
				}
				}
				break;
			case GTE_ANY:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(657);
				match(GTE_ANY);
				}
				}
				break;
			case LT_ANY:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(658);
				match(LT_ANY);
				}
				}
				break;
			case LTE_ANY:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(659);
				match(LTE_ANY);
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

	public static class Add_exprContext extends ParserRuleContext {
		public List<Multiply_exprContext> multiply_expr() {
			return getRuleContexts(Multiply_exprContext.class);
		}
		public Multiply_exprContext multiply_expr(int i) {
			return getRuleContext(Multiply_exprContext.class,i);
		}
		public List<TerminalNode> PLUS() { return getTokens(KVQLParser.PLUS); }
		public TerminalNode PLUS(int i) {
			return getToken(KVQLParser.PLUS, i);
		}
		public List<TerminalNode> MINUS() { return getTokens(KVQLParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(KVQLParser.MINUS, i);
		}
		public Add_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_expr(this);
		}
	}

	public final Add_exprContext add_expr() throws RecognitionException {
		Add_exprContext _localctx = new Add_exprContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_add_expr);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(662);
			multiply_expr();
			setState(667);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,46,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(663);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(664);
					multiply_expr();
					}
					} 
				}
				setState(669);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,46,_ctx);
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

	public static class Multiply_exprContext extends ParserRuleContext {
		public List<Unary_exprContext> unary_expr() {
			return getRuleContexts(Unary_exprContext.class);
		}
		public Unary_exprContext unary_expr(int i) {
			return getRuleContext(Unary_exprContext.class,i);
		}
		public List<TerminalNode> STAR() { return getTokens(KVQLParser.STAR); }
		public TerminalNode STAR(int i) {
			return getToken(KVQLParser.STAR, i);
		}
		public List<TerminalNode> DIV() { return getTokens(KVQLParser.DIV); }
		public TerminalNode DIV(int i) {
			return getToken(KVQLParser.DIV, i);
		}
		public Multiply_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiply_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMultiply_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMultiply_expr(this);
		}
	}

	public final Multiply_exprContext multiply_expr() throws RecognitionException {
		Multiply_exprContext _localctx = new Multiply_exprContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_multiply_expr);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(670);
			unary_expr();
			setState(675);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,47,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(671);
					_la = _input.LA(1);
					if ( !(_la==STAR || _la==DIV) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(672);
					unary_expr();
					}
					} 
				}
				setState(677);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,47,_ctx);
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

	public static class Unary_exprContext extends ParserRuleContext {
		public Path_exprContext path_expr() {
			return getRuleContext(Path_exprContext.class,0);
		}
		public Unary_exprContext unary_expr() {
			return getRuleContext(Unary_exprContext.class,0);
		}
		public TerminalNode PLUS() { return getToken(KVQLParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(KVQLParser.MINUS, 0); }
		public Unary_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUnary_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUnary_expr(this);
		}
	}

	public final Unary_exprContext unary_expr() throws RecognitionException {
		Unary_exprContext _localctx = new Unary_exprContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_unary_expr);
		int _la;
		try {
			setState(681);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(678);
				path_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(679);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(680);
				unary_expr();
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

	public static class Path_exprContext extends ParserRuleContext {
		public Primary_exprContext primary_expr() {
			return getRuleContext(Primary_exprContext.class,0);
		}
		public List<Map_stepContext> map_step() {
			return getRuleContexts(Map_stepContext.class);
		}
		public Map_stepContext map_step(int i) {
			return getRuleContext(Map_stepContext.class,i);
		}
		public List<Array_stepContext> array_step() {
			return getRuleContexts(Array_stepContext.class);
		}
		public Array_stepContext array_step(int i) {
			return getRuleContext(Array_stepContext.class,i);
		}
		public Path_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPath_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPath_expr(this);
		}
	}

	public final Path_exprContext path_expr() throws RecognitionException {
		Path_exprContext _localctx = new Path_exprContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_path_expr);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(683);
			primary_expr();
			setState(688);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,50,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(686);
					switch (_input.LA(1)) {
					case DOT:
						{
						setState(684);
						map_step();
						}
						break;
					case LBRACK:
						{
						setState(685);
						array_step();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					} 
				}
				setState(690);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,50,_ctx);
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

	public static class Map_stepContext extends ParserRuleContext {
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public Map_filter_stepContext map_filter_step() {
			return getRuleContext(Map_filter_stepContext.class,0);
		}
		public Map_field_stepContext map_field_step() {
			return getRuleContext(Map_field_stepContext.class,0);
		}
		public Map_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_step(this);
		}
	}

	public final Map_stepContext map_step() throws RecognitionException {
		Map_stepContext _localctx = new Map_stepContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_map_step);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(691);
			match(DOT);
			setState(694);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
			case 1:
				{
				setState(692);
				map_filter_step();
				}
				break;
			case 2:
				{
				setState(693);
				map_field_step();
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

	public static class Map_field_stepContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public Var_refContext var_ref() {
			return getRuleContext(Var_refContext.class,0);
		}
		public Parenthesized_exprContext parenthesized_expr() {
			return getRuleContext(Parenthesized_exprContext.class,0);
		}
		public Func_callContext func_call() {
			return getRuleContext(Func_callContext.class,0);
		}
		public Map_field_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_field_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_field_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_field_step(this);
		}
	}

	public final Map_field_stepContext map_field_step() throws RecognitionException {
		Map_field_stepContext _localctx = new Map_field_stepContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_map_field_step);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(701);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
			case 1:
				{
				setState(696);
				id();
				}
				break;
			case 2:
				{
				setState(697);
				string();
				}
				break;
			case 3:
				{
				setState(698);
				var_ref();
				}
				break;
			case 4:
				{
				setState(699);
				parenthesized_expr();
				}
				break;
			case 5:
				{
				setState(700);
				func_call();
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

	public static class Map_filter_stepContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Map_filter_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_filter_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_filter_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_filter_step(this);
		}
	}

	public final Map_filter_stepContext map_filter_step() throws RecognitionException {
		Map_filter_stepContext _localctx = new Map_filter_stepContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_map_filter_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(703);
			_la = _input.LA(1);
			if ( !(_la==KEYS || _la==VALUES) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(704);
			match(LP);
			setState(706);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)) | (1L << (LP - 64)) | (1L << (LBRACK - 64)) | (1L << (LBRACE - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DOLLAR - 130)) | (1L << (PLUS - 130)) | (1L << (MINUS - 130)) | (1L << (NULL - 130)) | (1L << (FALSE - 130)) | (1L << (TRUE - 130)) | (1L << (INT - 130)) | (1L << (FLOAT - 130)) | (1L << (NUMBER - 130)) | (1L << (DSTRING - 130)) | (1L << (STRING - 130)) | (1L << (ID - 130)) | (1L << (BAD_ID - 130)))) != 0)) {
				{
				setState(705);
				expr();
				}
			}

			setState(708);
			match(RP);
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

	public static class Array_stepContext extends ParserRuleContext {
		public Array_filter_stepContext array_filter_step() {
			return getRuleContext(Array_filter_stepContext.class,0);
		}
		public Array_slice_stepContext array_slice_step() {
			return getRuleContext(Array_slice_stepContext.class,0);
		}
		public Array_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_step(this);
		}
	}

	public final Array_stepContext array_step() throws RecognitionException {
		Array_stepContext _localctx = new Array_stepContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_array_step);
		try {
			setState(712);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(710);
				array_filter_step();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(711);
				array_slice_step();
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

	public static class Array_slice_stepContext extends ParserRuleContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode COLON() { return getToken(KVQLParser.COLON, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public Array_slice_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_slice_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_slice_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_slice_step(this);
		}
	}

	public final Array_slice_stepContext array_slice_step() throws RecognitionException {
		Array_slice_stepContext _localctx = new Array_slice_stepContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_array_slice_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(714);
			match(LBRACK);
			setState(716);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)) | (1L << (LP - 64)) | (1L << (LBRACK - 64)) | (1L << (LBRACE - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DOLLAR - 130)) | (1L << (PLUS - 130)) | (1L << (MINUS - 130)) | (1L << (NULL - 130)) | (1L << (FALSE - 130)) | (1L << (TRUE - 130)) | (1L << (INT - 130)) | (1L << (FLOAT - 130)) | (1L << (NUMBER - 130)) | (1L << (DSTRING - 130)) | (1L << (STRING - 130)) | (1L << (ID - 130)) | (1L << (BAD_ID - 130)))) != 0)) {
				{
				setState(715);
				expr();
				}
			}

			setState(718);
			match(COLON);
			setState(720);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)) | (1L << (LP - 64)) | (1L << (LBRACK - 64)) | (1L << (LBRACE - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DOLLAR - 130)) | (1L << (PLUS - 130)) | (1L << (MINUS - 130)) | (1L << (NULL - 130)) | (1L << (FALSE - 130)) | (1L << (TRUE - 130)) | (1L << (INT - 130)) | (1L << (FLOAT - 130)) | (1L << (NUMBER - 130)) | (1L << (DSTRING - 130)) | (1L << (STRING - 130)) | (1L << (ID - 130)) | (1L << (BAD_ID - 130)))) != 0)) {
				{
				setState(719);
				expr();
				}
			}

			setState(722);
			match(RBRACK);
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

	public static class Array_filter_stepContext extends ParserRuleContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Array_filter_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_filter_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_filter_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_filter_step(this);
		}
	}

	public final Array_filter_stepContext array_filter_step() throws RecognitionException {
		Array_filter_stepContext _localctx = new Array_filter_stepContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_array_filter_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(724);
			match(LBRACK);
			setState(726);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)) | (1L << (LP - 64)) | (1L << (LBRACK - 64)) | (1L << (LBRACE - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DOLLAR - 130)) | (1L << (PLUS - 130)) | (1L << (MINUS - 130)) | (1L << (NULL - 130)) | (1L << (FALSE - 130)) | (1L << (TRUE - 130)) | (1L << (INT - 130)) | (1L << (FLOAT - 130)) | (1L << (NUMBER - 130)) | (1L << (DSTRING - 130)) | (1L << (STRING - 130)) | (1L << (ID - 130)) | (1L << (BAD_ID - 130)))) != 0)) {
				{
				setState(725);
				expr();
				}
			}

			setState(728);
			match(RBRACK);
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

	public static class Primary_exprContext extends ParserRuleContext {
		public Const_exprContext const_expr() {
			return getRuleContext(Const_exprContext.class,0);
		}
		public Column_refContext column_ref() {
			return getRuleContext(Column_refContext.class,0);
		}
		public Var_refContext var_ref() {
			return getRuleContext(Var_refContext.class,0);
		}
		public Array_constructorContext array_constructor() {
			return getRuleContext(Array_constructorContext.class,0);
		}
		public Map_constructorContext map_constructor() {
			return getRuleContext(Map_constructorContext.class,0);
		}
		public Transform_exprContext transform_expr() {
			return getRuleContext(Transform_exprContext.class,0);
		}
		public Func_callContext func_call() {
			return getRuleContext(Func_callContext.class,0);
		}
		public Count_starContext count_star() {
			return getRuleContext(Count_starContext.class,0);
		}
		public Case_exprContext case_expr() {
			return getRuleContext(Case_exprContext.class,0);
		}
		public Cast_exprContext cast_expr() {
			return getRuleContext(Cast_exprContext.class,0);
		}
		public Parenthesized_exprContext parenthesized_expr() {
			return getRuleContext(Parenthesized_exprContext.class,0);
		}
		public Extract_exprContext extract_expr() {
			return getRuleContext(Extract_exprContext.class,0);
		}
		public Primary_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primary_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPrimary_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPrimary_expr(this);
		}
	}

	public final Primary_exprContext primary_expr() throws RecognitionException {
		Primary_exprContext _localctx = new Primary_exprContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_primary_expr);
		try {
			setState(742);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(730);
				const_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(731);
				column_ref();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(732);
				var_ref();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(733);
				array_constructor();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(734);
				map_constructor();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(735);
				transform_expr();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(736);
				func_call();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(737);
				count_star();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(738);
				case_expr();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(739);
				cast_expr();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(740);
				parenthesized_expr();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(741);
				extract_expr();
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

	public static class Column_refContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public Column_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_ref; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterColumn_ref(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitColumn_ref(this);
		}
	}

	public final Column_refContext column_ref() throws RecognitionException {
		Column_refContext _localctx = new Column_refContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_column_ref);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(744);
			id();
			setState(747);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
			case 1:
				{
				setState(745);
				match(DOT);
				setState(746);
				id();
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

	public static class Const_exprContext extends ParserRuleContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public Const_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_const_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterConst_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitConst_expr(this);
		}
	}

	public final Const_exprContext const_expr() throws RecognitionException {
		Const_exprContext _localctx = new Const_exprContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_const_expr);
		try {
			setState(754);
			switch (_input.LA(1)) {
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				enterOuterAlt(_localctx, 1);
				{
				setState(749);
				number();
				}
				break;
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(750);
				string();
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 3);
				{
				setState(751);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 4);
				{
				setState(752);
				match(FALSE);
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 5);
				{
				setState(753);
				match(NULL);
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

	public static class Var_refContext extends ParserRuleContext {
		public TerminalNode DOLLAR() { return getToken(KVQLParser.DOLLAR, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Var_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_var_ref; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterVar_ref(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitVar_ref(this);
		}
	}

	public final Var_refContext var_ref() throws RecognitionException {
		Var_refContext _localctx = new Var_refContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_var_ref);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(756);
			match(DOLLAR);
			setState(758);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
			case 1:
				{
				setState(757);
				id();
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

	public static class Array_constructorContext extends ParserRuleContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Array_constructorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_constructor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_constructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_constructor(this);
		}
	}

	public final Array_constructorContext array_constructor() throws RecognitionException {
		Array_constructorContext _localctx = new Array_constructorContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_array_constructor);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(760);
			match(LBRACK);
			setState(762);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)) | (1L << (LP - 64)) | (1L << (LBRACK - 64)) | (1L << (LBRACE - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DOLLAR - 130)) | (1L << (PLUS - 130)) | (1L << (MINUS - 130)) | (1L << (NULL - 130)) | (1L << (FALSE - 130)) | (1L << (TRUE - 130)) | (1L << (INT - 130)) | (1L << (FLOAT - 130)) | (1L << (NUMBER - 130)) | (1L << (DSTRING - 130)) | (1L << (STRING - 130)) | (1L << (ID - 130)) | (1L << (BAD_ID - 130)))) != 0)) {
				{
				setState(761);
				expr();
				}
			}

			setState(768);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(764);
				match(COMMA);
				setState(765);
				expr();
				}
				}
				setState(770);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(771);
			match(RBRACK);
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

	public static class Map_constructorContext extends ParserRuleContext {
		public TerminalNode LBRACE() { return getToken(KVQLParser.LBRACE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COLON() { return getTokens(KVQLParser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(KVQLParser.COLON, i);
		}
		public TerminalNode RBRACE() { return getToken(KVQLParser.RBRACE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Map_constructorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_constructor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_constructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_constructor(this);
		}
	}

	public final Map_constructorContext map_constructor() throws RecognitionException {
		Map_constructorContext _localctx = new Map_constructorContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_map_constructor);
		int _la;
		try {
			setState(791);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(773);
				match(LBRACE);
				setState(774);
				expr();
				setState(775);
				match(COLON);
				setState(776);
				expr();
				setState(784);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(777);
					match(COMMA);
					setState(778);
					expr();
					setState(779);
					match(COLON);
					setState(780);
					expr();
					}
					}
					setState(786);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(787);
				match(RBRACE);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(789);
				match(LBRACE);
				setState(790);
				match(RBRACE);
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

	public static class Transform_exprContext extends ParserRuleContext {
		public TerminalNode SEQ_TRANSFORM() { return getToken(KVQLParser.SEQ_TRANSFORM, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Transform_input_exprContext transform_input_expr() {
			return getRuleContext(Transform_input_exprContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(KVQLParser.COMMA, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Transform_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTransform_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTransform_expr(this);
		}
	}

	public final Transform_exprContext transform_expr() throws RecognitionException {
		Transform_exprContext _localctx = new Transform_exprContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_transform_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(793);
			match(SEQ_TRANSFORM);
			setState(794);
			match(LP);
			setState(795);
			transform_input_expr();
			setState(796);
			match(COMMA);
			setState(797);
			expr();
			setState(798);
			match(RP);
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

	public static class Transform_input_exprContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Transform_input_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform_input_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTransform_input_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTransform_input_expr(this);
		}
	}

	public final Transform_input_exprContext transform_input_expr() throws RecognitionException {
		Transform_input_exprContext _localctx = new Transform_input_exprContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_transform_input_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(800);
			expr();
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

	public static class Func_callContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Func_callContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_call; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFunc_call(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFunc_call(this);
		}
	}

	public final Func_callContext func_call() throws RecognitionException {
		Func_callContext _localctx = new Func_callContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_func_call);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(802);
			id();
			setState(803);
			match(LP);
			setState(812);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)) | (1L << (LP - 64)) | (1L << (LBRACK - 64)) | (1L << (LBRACE - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DOLLAR - 130)) | (1L << (PLUS - 130)) | (1L << (MINUS - 130)) | (1L << (NULL - 130)) | (1L << (FALSE - 130)) | (1L << (TRUE - 130)) | (1L << (INT - 130)) | (1L << (FLOAT - 130)) | (1L << (NUMBER - 130)) | (1L << (DSTRING - 130)) | (1L << (STRING - 130)) | (1L << (ID - 130)) | (1L << (BAD_ID - 130)))) != 0)) {
				{
				setState(804);
				expr();
				setState(809);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(805);
					match(COMMA);
					setState(806);
					expr();
					}
					}
					setState(811);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(814);
			match(RP);
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

	public static class Count_starContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(KVQLParser.COUNT, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode STAR() { return getToken(KVQLParser.STAR, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Count_starContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_count_star; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCount_star(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCount_star(this);
		}
	}

	public final Count_starContext count_star() throws RecognitionException {
		Count_starContext _localctx = new Count_starContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_count_star);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(816);
			match(COUNT);
			setState(817);
			match(LP);
			setState(818);
			match(STAR);
			setState(819);
			match(RP);
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

	public static class Case_exprContext extends ParserRuleContext {
		public TerminalNode CASE() { return getToken(KVQLParser.CASE, 0); }
		public List<TerminalNode> WHEN() { return getTokens(KVQLParser.WHEN); }
		public TerminalNode WHEN(int i) {
			return getToken(KVQLParser.WHEN, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> THEN() { return getTokens(KVQLParser.THEN); }
		public TerminalNode THEN(int i) {
			return getToken(KVQLParser.THEN, i);
		}
		public TerminalNode END() { return getToken(KVQLParser.END, 0); }
		public TerminalNode ELSE() { return getToken(KVQLParser.ELSE, 0); }
		public Case_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_case_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCase_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCase_expr(this);
		}
	}

	public final Case_exprContext case_expr() throws RecognitionException {
		Case_exprContext _localctx = new Case_exprContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_case_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(821);
			match(CASE);
			setState(822);
			match(WHEN);
			setState(823);
			expr();
			setState(824);
			match(THEN);
			setState(825);
			expr();
			setState(833);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WHEN) {
				{
				{
				setState(826);
				match(WHEN);
				setState(827);
				expr();
				setState(828);
				match(THEN);
				setState(829);
				expr();
				}
				}
				setState(835);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(838);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(836);
				match(ELSE);
				setState(837);
				expr();
				}
			}

			setState(840);
			match(END);
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

	public static class Cast_exprContext extends ParserRuleContext {
		public TerminalNode CAST() { return getToken(KVQLParser.CAST, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Quantified_type_defContext quantified_type_def() {
			return getRuleContext(Quantified_type_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Cast_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cast_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCast_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCast_expr(this);
		}
	}

	public final Cast_exprContext cast_expr() throws RecognitionException {
		Cast_exprContext _localctx = new Cast_exprContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_cast_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(842);
			match(CAST);
			setState(843);
			match(LP);
			setState(844);
			expr();
			setState(845);
			match(AS);
			setState(846);
			quantified_type_def();
			setState(847);
			match(RP);
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

	public static class Parenthesized_exprContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Parenthesized_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parenthesized_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterParenthesized_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitParenthesized_expr(this);
		}
	}

	public final Parenthesized_exprContext parenthesized_expr() throws RecognitionException {
		Parenthesized_exprContext _localctx = new Parenthesized_exprContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_parenthesized_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(849);
			match(LP);
			setState(850);
			expr();
			setState(851);
			match(RP);
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

	public static class Extract_exprContext extends ParserRuleContext {
		public TerminalNode EXTRACT() { return getToken(KVQLParser.EXTRACT, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Extract_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extract_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterExtract_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitExtract_expr(this);
		}
	}

	public final Extract_exprContext extract_expr() throws RecognitionException {
		Extract_exprContext _localctx = new Extract_exprContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_extract_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(853);
			match(EXTRACT);
			setState(854);
			match(LP);
			setState(855);
			id();
			setState(856);
			match(FROM);
			setState(857);
			expr();
			setState(858);
			match(RP);
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

	public static class Update_statementContext extends ParserRuleContext {
		public TerminalNode UPDATE() { return getToken(KVQLParser.UPDATE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Update_clauseContext> update_clause() {
			return getRuleContexts(Update_clauseContext.class);
		}
		public Update_clauseContext update_clause(int i) {
			return getRuleContext(Update_clauseContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Tab_aliasContext tab_alias() {
			return getRuleContext(Tab_aliasContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Returning_clauseContext returning_clause() {
			return getRuleContext(Returning_clauseContext.class,0);
		}
		public Update_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUpdate_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUpdate_statement(this);
		}
	}

	public final Update_statementContext update_statement() throws RecognitionException {
		Update_statementContext _localctx = new Update_statementContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_update_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(861);
			_la = _input.LA(1);
			if (_la==DECLARE) {
				{
				setState(860);
				prolog();
				}
			}

			setState(863);
			match(UPDATE);
			setState(864);
			table_name();
			setState(866);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
			case 1:
				{
				setState(865);
				match(AS);
				}
				break;
			}
			setState(869);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				{
				setState(868);
				tab_alias();
				}
				break;
			}
			setState(871);
			update_clause();
			setState(876);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(872);
				match(COMMA);
				setState(873);
				update_clause();
				}
				}
				setState(878);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(879);
			match(WHERE);
			setState(880);
			expr();
			setState(882);
			_la = _input.LA(1);
			if (_la==RETURNING) {
				{
				setState(881);
				returning_clause();
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

	public static class Returning_clauseContext extends ParserRuleContext {
		public TerminalNode RETURNING() { return getToken(KVQLParser.RETURNING, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public Returning_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_returning_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterReturning_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitReturning_clause(this);
		}
	}

	public final Returning_clauseContext returning_clause() throws RecognitionException {
		Returning_clauseContext _localctx = new Returning_clauseContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_returning_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(884);
			match(RETURNING);
			setState(885);
			select_list();
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

	public static class Update_clauseContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(KVQLParser.SET, 0); }
		public List<Set_clauseContext> set_clause() {
			return getRuleContexts(Set_clauseContext.class);
		}
		public Set_clauseContext set_clause(int i) {
			return getRuleContext(Set_clauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public List<Update_clauseContext> update_clause() {
			return getRuleContexts(Update_clauseContext.class);
		}
		public Update_clauseContext update_clause(int i) {
			return getRuleContext(Update_clauseContext.class,i);
		}
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public List<Add_clauseContext> add_clause() {
			return getRuleContexts(Add_clauseContext.class);
		}
		public Add_clauseContext add_clause(int i) {
			return getRuleContext(Add_clauseContext.class,i);
		}
		public TerminalNode PUT() { return getToken(KVQLParser.PUT, 0); }
		public List<Put_clauseContext> put_clause() {
			return getRuleContexts(Put_clauseContext.class);
		}
		public Put_clauseContext put_clause(int i) {
			return getRuleContext(Put_clauseContext.class,i);
		}
		public TerminalNode REMOVE() { return getToken(KVQLParser.REMOVE, 0); }
		public List<Remove_clauseContext> remove_clause() {
			return getRuleContexts(Remove_clauseContext.class);
		}
		public Remove_clauseContext remove_clause(int i) {
			return getRuleContext(Remove_clauseContext.class,i);
		}
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public Ttl_clauseContext ttl_clause() {
			return getRuleContext(Ttl_clauseContext.class,0);
		}
		public Update_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUpdate_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUpdate_clause(this);
		}
	}

	public final Update_clauseContext update_clause() throws RecognitionException {
		Update_clauseContext _localctx = new Update_clauseContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_update_clause);
		try {
			int _alt;
			setState(942);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(887);
				match(SET);
				setState(888);
				set_clause();
				setState(896);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,76,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(889);
						match(COMMA);
						setState(892);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
						case 1:
							{
							setState(890);
							update_clause();
							}
							break;
						case 2:
							{
							setState(891);
							set_clause();
							}
							break;
						}
						}
						} 
					}
					setState(898);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,76,_ctx);
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(899);
				match(ADD);
				setState(900);
				add_clause();
				setState(908);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,78,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(901);
						match(COMMA);
						setState(904);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
						case 1:
							{
							setState(902);
							update_clause();
							}
							break;
						case 2:
							{
							setState(903);
							add_clause();
							}
							break;
						}
						}
						} 
					}
					setState(910);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,78,_ctx);
				}
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(911);
				match(PUT);
				setState(912);
				put_clause();
				setState(920);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(913);
						match(COMMA);
						setState(916);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
						case 1:
							{
							setState(914);
							update_clause();
							}
							break;
						case 2:
							{
							setState(915);
							put_clause();
							}
							break;
						}
						}
						} 
					}
					setState(922);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
				}
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(923);
				match(REMOVE);
				setState(924);
				remove_clause();
				setState(929);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,81,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(925);
						match(COMMA);
						setState(926);
						remove_clause();
						}
						} 
					}
					setState(931);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,81,_ctx);
				}
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(932);
				match(SET);
				setState(933);
				match(TTL);
				setState(934);
				ttl_clause();
				setState(939);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,82,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(935);
						match(COMMA);
						setState(936);
						update_clause();
						}
						} 
					}
					setState(941);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,82,_ctx);
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

	public static class Set_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public TerminalNode EQ() { return getToken(KVQLParser.EQ, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Set_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSet_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSet_clause(this);
		}
	}

	public final Set_clauseContext set_clause() throws RecognitionException {
		Set_clauseContext _localctx = new Set_clauseContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_set_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(944);
			target_expr();
			setState(945);
			match(EQ);
			setState(946);
			expr();
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

	public static class Add_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Pos_exprContext pos_expr() {
			return getRuleContext(Pos_exprContext.class,0);
		}
		public Add_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_clause(this);
		}
	}

	public final Add_clauseContext add_clause() throws RecognitionException {
		Add_clauseContext _localctx = new Add_clauseContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_add_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(948);
			target_expr();
			setState(950);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
			case 1:
				{
				setState(949);
				pos_expr();
				}
				break;
			}
			setState(952);
			expr();
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

	public static class Put_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Put_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_put_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPut_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPut_clause(this);
		}
	}

	public final Put_clauseContext put_clause() throws RecognitionException {
		Put_clauseContext _localctx = new Put_clauseContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_put_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(954);
			target_expr();
			setState(955);
			expr();
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

	public static class Remove_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public Remove_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_remove_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRemove_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRemove_clause(this);
		}
	}

	public final Remove_clauseContext remove_clause() throws RecognitionException {
		Remove_clauseContext _localctx = new Remove_clauseContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_remove_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(957);
			target_expr();
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

	public static class Ttl_clauseContext extends ParserRuleContext {
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public Ttl_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ttl_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTtl_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTtl_clause(this);
		}
	}

	public final Ttl_clauseContext ttl_clause() throws RecognitionException {
		Ttl_clauseContext _localctx = new Ttl_clauseContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_ttl_clause);
		int _la;
		try {
			setState(965);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(959);
				add_expr();
				setState(960);
				_la = _input.LA(1);
				if ( !(_la==DAYS || _la==HOURS) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(962);
				match(USING);
				setState(963);
				match(TABLE);
				setState(964);
				match(DEFAULT);
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

	public static class Target_exprContext extends ParserRuleContext {
		public Path_exprContext path_expr() {
			return getRuleContext(Path_exprContext.class,0);
		}
		public Target_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_target_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTarget_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTarget_expr(this);
		}
	}

	public final Target_exprContext target_expr() throws RecognitionException {
		Target_exprContext _localctx = new Target_exprContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_target_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(967);
			path_expr();
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

	public static class Pos_exprContext extends ParserRuleContext {
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Pos_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pos_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPos_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPos_expr(this);
		}
	}

	public final Pos_exprContext pos_expr() throws RecognitionException {
		Pos_exprContext _localctx = new Pos_exprContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_pos_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(969);
			add_expr();
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

	public static class Quantified_type_defContext extends ParserRuleContext {
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public TerminalNode STAR() { return getToken(KVQLParser.STAR, 0); }
		public TerminalNode PLUS() { return getToken(KVQLParser.PLUS, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(KVQLParser.QUESTION_MARK, 0); }
		public Quantified_type_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quantified_type_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterQuantified_type_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitQuantified_type_def(this);
		}
	}

	public final Quantified_type_defContext quantified_type_def() throws RecognitionException {
		Quantified_type_defContext _localctx = new Quantified_type_defContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_quantified_type_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(971);
			type_def();
			setState(973);
			_la = _input.LA(1);
			if (((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STAR - 128)) | (1L << (QUESTION_MARK - 128)) | (1L << (PLUS - 128)))) != 0)) {
				{
				setState(972);
				_la = _input.LA(1);
				if ( !(((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STAR - 128)) | (1L << (QUESTION_MARK - 128)) | (1L << (PLUS - 128)))) != 0)) ) {
				_errHandler.recoverInline(this);
				} else {
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

	public static class Type_defContext extends ParserRuleContext {
		public Type_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type_def; }
	 
		public Type_defContext() { }
		public void copyFrom(Type_defContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class EnumContext extends Type_defContext {
		public Enum_defContext enum_def() {
			return getRuleContext(Enum_defContext.class,0);
		}
		public EnumContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEnum(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEnum(this);
		}
	}
	public static class AnyAtomicContext extends Type_defContext {
		public AnyAtomic_defContext anyAtomic_def() {
			return getRuleContext(AnyAtomic_defContext.class,0);
		}
		public AnyAtomicContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyAtomic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyAtomic(this);
		}
	}
	public static class AnyJsonAtomicContext extends Type_defContext {
		public AnyJsonAtomic_defContext anyJsonAtomic_def() {
			return getRuleContext(AnyJsonAtomic_defContext.class,0);
		}
		public AnyJsonAtomicContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyJsonAtomic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyJsonAtomic(this);
		}
	}
	public static class AnyRecordContext extends Type_defContext {
		public AnyRecord_defContext anyRecord_def() {
			return getRuleContext(AnyRecord_defContext.class,0);
		}
		public AnyRecordContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyRecord(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyRecord(this);
		}
	}
	public static class JSONContext extends Type_defContext {
		public Json_defContext json_def() {
			return getRuleContext(Json_defContext.class,0);
		}
		public JSONContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJSON(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJSON(this);
		}
	}
	public static class StringTContext extends Type_defContext {
		public String_defContext string_def() {
			return getRuleContext(String_defContext.class,0);
		}
		public StringTContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterStringT(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitStringT(this);
		}
	}
	public static class TimestampContext extends Type_defContext {
		public Timestamp_defContext timestamp_def() {
			return getRuleContext(Timestamp_defContext.class,0);
		}
		public TimestampContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTimestamp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTimestamp(this);
		}
	}
	public static class AnyContext extends Type_defContext {
		public Any_defContext any_def() {
			return getRuleContext(Any_defContext.class,0);
		}
		public AnyContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAny(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAny(this);
		}
	}
	public static class IntContext extends Type_defContext {
		public Integer_defContext integer_def() {
			return getRuleContext(Integer_defContext.class,0);
		}
		public IntContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInt(this);
		}
	}
	public static class ArrayContext extends Type_defContext {
		public Array_defContext array_def() {
			return getRuleContext(Array_defContext.class,0);
		}
		public ArrayContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray(this);
		}
	}
	public static class FloatContext extends Type_defContext {
		public Float_defContext float_def() {
			return getRuleContext(Float_defContext.class,0);
		}
		public FloatContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFloat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFloat(this);
		}
	}
	public static class RecordContext extends Type_defContext {
		public Record_defContext record_def() {
			return getRuleContext(Record_defContext.class,0);
		}
		public RecordContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRecord(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRecord(this);
		}
	}
	public static class BinaryContext extends Type_defContext {
		public Binary_defContext binary_def() {
			return getRuleContext(Binary_defContext.class,0);
		}
		public BinaryContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBinary(this);
		}
	}
	public static class BooleanContext extends Type_defContext {
		public Boolean_defContext boolean_def() {
			return getRuleContext(Boolean_defContext.class,0);
		}
		public BooleanContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBoolean(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBoolean(this);
		}
	}
	public static class MapContext extends Type_defContext {
		public Map_defContext map_def() {
			return getRuleContext(Map_defContext.class,0);
		}
		public MapContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap(this);
		}
	}

	public final Type_defContext type_def() throws RecognitionException {
		Type_defContext _localctx = new Type_defContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_type_def);
		try {
			setState(990);
			switch (_input.LA(1)) {
			case BINARY_T:
				_localctx = new BinaryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(975);
				binary_def();
				}
				break;
			case ARRAY_T:
				_localctx = new ArrayContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(976);
				array_def();
				}
				break;
			case BOOLEAN_T:
				_localctx = new BooleanContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(977);
				boolean_def();
				}
				break;
			case ENUM_T:
				_localctx = new EnumContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(978);
				enum_def();
				}
				break;
			case DOUBLE_T:
			case FLOAT_T:
			case NUMBER_T:
				_localctx = new FloatContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(979);
				float_def();
				}
				break;
			case INTEGER_T:
			case LONG_T:
				_localctx = new IntContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(980);
				integer_def();
				}
				break;
			case JSON:
				_localctx = new JSONContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(981);
				json_def();
				}
				break;
			case MAP_T:
				_localctx = new MapContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(982);
				map_def();
				}
				break;
			case RECORD_T:
				_localctx = new RecordContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(983);
				record_def();
				}
				break;
			case STRING_T:
				_localctx = new StringTContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(984);
				string_def();
				}
				break;
			case TIMESTAMP_T:
				_localctx = new TimestampContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(985);
				timestamp_def();
				}
				break;
			case ANY_T:
				_localctx = new AnyContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(986);
				any_def();
				}
				break;
			case ANYATOMIC_T:
				_localctx = new AnyAtomicContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(987);
				anyAtomic_def();
				}
				break;
			case ANYJSONATOMIC_T:
				_localctx = new AnyJsonAtomicContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(988);
				anyJsonAtomic_def();
				}
				break;
			case ANYRECORD_T:
				_localctx = new AnyRecordContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(989);
				anyRecord_def();
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

	public static class Record_defContext extends ParserRuleContext {
		public TerminalNode RECORD_T() { return getToken(KVQLParser.RECORD_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Field_defContext> field_def() {
			return getRuleContexts(Field_defContext.class);
		}
		public Field_defContext field_def(int i) {
			return getRuleContext(Field_defContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Record_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_record_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRecord_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRecord_def(this);
		}
	}

	public final Record_defContext record_def() throws RecognitionException {
		Record_defContext _localctx = new Record_defContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_record_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(992);
			match(RECORD_T);
			setState(993);
			match(LP);
			setState(994);
			field_def();
			setState(999);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(995);
				match(COMMA);
				setState(996);
				field_def();
				}
				}
				setState(1001);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1002);
			match(RP);
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

	public static class Field_defContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Field_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterField_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitField_def(this);
		}
	}

	public final Field_defContext field_def() throws RecognitionException {
		Field_defContext _localctx = new Field_defContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_field_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1004);
			id();
			setState(1005);
			type_def();
			setState(1007);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==NOT) {
				{
				setState(1006);
				default_def();
				}
			}

			setState(1010);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1009);
				comment();
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

	public static class Default_defContext extends ParserRuleContext {
		public Default_valueContext default_value() {
			return getRuleContext(Default_valueContext.class,0);
		}
		public Not_nullContext not_null() {
			return getRuleContext(Not_nullContext.class,0);
		}
		public Default_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_default_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDefault_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDefault_def(this);
		}
	}

	public final Default_defContext default_def() throws RecognitionException {
		Default_defContext _localctx = new Default_defContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_default_def);
		int _la;
		try {
			setState(1020);
			switch (_input.LA(1)) {
			case DEFAULT:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1012);
				default_value();
				setState(1014);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1013);
					not_null();
					}
				}

				}
				}
				break;
			case NOT:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1016);
				not_null();
				setState(1018);
				_la = _input.LA(1);
				if (_la==DEFAULT) {
					{
					setState(1017);
					default_value();
					}
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

	public static class Default_valueContext extends ParserRuleContext {
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Default_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_default_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDefault_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDefault_value(this);
		}
	}

	public final Default_valueContext default_value() throws RecognitionException {
		Default_valueContext _localctx = new Default_valueContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_default_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1022);
			match(DEFAULT);
			setState(1028);
			switch (_input.LA(1)) {
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				{
				setState(1023);
				number();
				}
				break;
			case DSTRING:
			case STRING:
				{
				setState(1024);
				string();
				}
				break;
			case TRUE:
				{
				setState(1025);
				match(TRUE);
				}
				break;
			case FALSE:
				{
				setState(1026);
				match(FALSE);
				}
				break;
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
			case BAD_ID:
				{
				setState(1027);
				id();
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class Not_nullContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public Not_nullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not_null; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNot_null(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNot_null(this);
		}
	}

	public final Not_nullContext not_null() throws RecognitionException {
		Not_nullContext _localctx = new Not_nullContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_not_null);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1030);
			match(NOT);
			setState(1031);
			match(NULL);
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

	public static class Map_defContext extends ParserRuleContext {
		public TerminalNode MAP_T() { return getToken(KVQLParser.MAP_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Map_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_def(this);
		}
	}

	public final Map_defContext map_def() throws RecognitionException {
		Map_defContext _localctx = new Map_defContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_map_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1033);
			match(MAP_T);
			setState(1034);
			match(LP);
			setState(1035);
			type_def();
			setState(1036);
			match(RP);
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

	public static class Array_defContext extends ParserRuleContext {
		public TerminalNode ARRAY_T() { return getToken(KVQLParser.ARRAY_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Array_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_def(this);
		}
	}

	public final Array_defContext array_def() throws RecognitionException {
		Array_defContext _localctx = new Array_defContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_array_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1038);
			match(ARRAY_T);
			setState(1039);
			match(LP);
			setState(1040);
			type_def();
			setState(1041);
			match(RP);
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

	public static class Integer_defContext extends ParserRuleContext {
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public Integer_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_integer_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInteger_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInteger_def(this);
		}
	}

	public final Integer_defContext integer_def() throws RecognitionException {
		Integer_defContext _localctx = new Integer_defContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_integer_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1043);
			_la = _input.LA(1);
			if ( !(_la==INTEGER_T || _la==LONG_T) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class Json_defContext extends ParserRuleContext {
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public Json_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_def(this);
		}
	}

	public final Json_defContext json_def() throws RecognitionException {
		Json_defContext _localctx = new Json_defContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_json_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1045);
			match(JSON);
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

	public static class Float_defContext extends ParserRuleContext {
		public TerminalNode FLOAT_T() { return getToken(KVQLParser.FLOAT_T, 0); }
		public TerminalNode DOUBLE_T() { return getToken(KVQLParser.DOUBLE_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public Float_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_float_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFloat_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFloat_def(this);
		}
	}

	public final Float_defContext float_def() throws RecognitionException {
		Float_defContext _localctx = new Float_defContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_float_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1047);
			_la = _input.LA(1);
			if ( !(((((_la - 104)) & ~0x3f) == 0 && ((1L << (_la - 104)) & ((1L << (DOUBLE_T - 104)) | (1L << (FLOAT_T - 104)) | (1L << (NUMBER_T - 104)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class String_defContext extends ParserRuleContext {
		public TerminalNode STRING_T() { return getToken(KVQLParser.STRING_T, 0); }
		public String_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterString_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitString_def(this);
		}
	}

	public final String_defContext string_def() throws RecognitionException {
		String_defContext _localctx = new String_defContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_string_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1049);
			match(STRING_T);
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

	public static class Enum_defContext extends ParserRuleContext {
		public TerminalNode ENUM_T() { return getToken(KVQLParser.ENUM_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Enum_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_enum_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEnum_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEnum_def(this);
		}
	}

	public final Enum_defContext enum_def() throws RecognitionException {
		Enum_defContext _localctx = new Enum_defContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_enum_def);
		try {
			setState(1061);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1051);
				match(ENUM_T);
				setState(1052);
				match(LP);
				setState(1053);
				id_list();
				setState(1054);
				match(RP);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1056);
				match(ENUM_T);
				setState(1057);
				match(LP);
				setState(1058);
				id_list();
				 notifyErrorListeners("Missing closing ')'"); 
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

	public static class Boolean_defContext extends ParserRuleContext {
		public TerminalNode BOOLEAN_T() { return getToken(KVQLParser.BOOLEAN_T, 0); }
		public Boolean_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_boolean_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBoolean_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBoolean_def(this);
		}
	}

	public final Boolean_defContext boolean_def() throws RecognitionException {
		Boolean_defContext _localctx = new Boolean_defContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_boolean_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1063);
			match(BOOLEAN_T);
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

	public static class Binary_defContext extends ParserRuleContext {
		public TerminalNode BINARY_T() { return getToken(KVQLParser.BINARY_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Binary_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_binary_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBinary_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBinary_def(this);
		}
	}

	public final Binary_defContext binary_def() throws RecognitionException {
		Binary_defContext _localctx = new Binary_defContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_binary_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1065);
			match(BINARY_T);
			setState(1069);
			_la = _input.LA(1);
			if (_la==LP) {
				{
				setState(1066);
				match(LP);
				setState(1067);
				match(INT);
				setState(1068);
				match(RP);
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

	public static class Timestamp_defContext extends ParserRuleContext {
		public TerminalNode TIMESTAMP_T() { return getToken(KVQLParser.TIMESTAMP_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Timestamp_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTimestamp_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTimestamp_def(this);
		}
	}

	public final Timestamp_defContext timestamp_def() throws RecognitionException {
		Timestamp_defContext _localctx = new Timestamp_defContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_timestamp_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1071);
			match(TIMESTAMP_T);
			setState(1075);
			_la = _input.LA(1);
			if (_la==LP) {
				{
				setState(1072);
				match(LP);
				setState(1073);
				match(INT);
				setState(1074);
				match(RP);
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

	public static class Any_defContext extends ParserRuleContext {
		public TerminalNode ANY_T() { return getToken(KVQLParser.ANY_T, 0); }
		public Any_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAny_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAny_def(this);
		}
	}

	public final Any_defContext any_def() throws RecognitionException {
		Any_defContext _localctx = new Any_defContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_any_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1077);
			match(ANY_T);
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

	public static class AnyAtomic_defContext extends ParserRuleContext {
		public TerminalNode ANYATOMIC_T() { return getToken(KVQLParser.ANYATOMIC_T, 0); }
		public AnyAtomic_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyAtomic_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyAtomic_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyAtomic_def(this);
		}
	}

	public final AnyAtomic_defContext anyAtomic_def() throws RecognitionException {
		AnyAtomic_defContext _localctx = new AnyAtomic_defContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_anyAtomic_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1079);
			match(ANYATOMIC_T);
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

	public static class AnyJsonAtomic_defContext extends ParserRuleContext {
		public TerminalNode ANYJSONATOMIC_T() { return getToken(KVQLParser.ANYJSONATOMIC_T, 0); }
		public AnyJsonAtomic_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyJsonAtomic_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyJsonAtomic_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyJsonAtomic_def(this);
		}
	}

	public final AnyJsonAtomic_defContext anyJsonAtomic_def() throws RecognitionException {
		AnyJsonAtomic_defContext _localctx = new AnyJsonAtomic_defContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_anyJsonAtomic_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1081);
			match(ANYJSONATOMIC_T);
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

	public static class AnyRecord_defContext extends ParserRuleContext {
		public TerminalNode ANYRECORD_T() { return getToken(KVQLParser.ANYRECORD_T, 0); }
		public AnyRecord_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyRecord_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyRecord_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyRecord_def(this);
		}
	}

	public final AnyRecord_defContext anyRecord_def() throws RecognitionException {
		AnyRecord_defContext _localctx = new AnyRecord_defContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_anyRecord_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1083);
			match(ANYRECORD_T);
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

	public static class Id_pathContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public Id_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_path(this);
		}
	}

	public final Id_pathContext id_path() throws RecognitionException {
		Id_pathContext _localctx = new Id_pathContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_id_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1085);
			id();
			setState(1090);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1086);
				match(DOT);
				setState(1087);
				id();
				}
				}
				setState(1092);
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

	public static class Name_pathContext extends ParserRuleContext {
		public List<Field_nameContext> field_name() {
			return getRuleContexts(Field_nameContext.class);
		}
		public Field_nameContext field_name(int i) {
			return getRuleContext(Field_nameContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public Name_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterName_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitName_path(this);
		}
	}

	public final Name_pathContext name_path() throws RecognitionException {
		Name_pathContext _localctx = new Name_pathContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_name_path);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1093);
			field_name();
			setState(1098);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,99,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1094);
					match(DOT);
					setState(1095);
					field_name();
					}
					} 
				}
				setState(1100);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,99,_ctx);
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

	public static class Field_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public Field_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterField_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitField_name(this);
		}
	}

	public final Field_nameContext field_name() throws RecognitionException {
		Field_nameContext _localctx = new Field_nameContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_field_name);
		try {
			setState(1103);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
			case BAD_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1101);
				id();
				}
				break;
			case DSTRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1102);
				match(DSTRING);
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

	public static class Create_table_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Table_defContext table_def() {
			return getRuleContext(Table_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Ttl_defContext ttl_def() {
			return getRuleContext(Ttl_defContext.class,0);
		}
		public Create_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_table_statement(this);
		}
	}

	public final Create_table_statementContext create_table_statement() throws RecognitionException {
		Create_table_statementContext _localctx = new Create_table_statementContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_create_table_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1105);
			match(CREATE);
			setState(1106);
			match(TABLE);
			setState(1110);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
			case 1:
				{
				setState(1107);
				match(IF);
				setState(1108);
				match(NOT);
				setState(1109);
				match(EXISTS);
				}
				break;
			}
			setState(1112);
			table_name();
			setState(1114);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1113);
				comment();
				}
			}

			setState(1116);
			match(LP);
			setState(1117);
			table_def();
			setState(1118);
			match(RP);
			setState(1120);
			_la = _input.LA(1);
			if (_la==USING) {
				{
				setState(1119);
				ttl_def();
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

	public static class Table_nameContext extends ParserRuleContext {
		public Id_pathContext id_path() {
			return getRuleContext(Id_pathContext.class,0);
		}
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_name(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1122);
			id_path();
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

	public static class Table_defContext extends ParserRuleContext {
		public List<Field_defContext> field_def() {
			return getRuleContexts(Field_defContext.class);
		}
		public Field_defContext field_def(int i) {
			return getRuleContext(Field_defContext.class,i);
		}
		public List<Key_defContext> key_def() {
			return getRuleContexts(Key_defContext.class);
		}
		public Key_defContext key_def(int i) {
			return getRuleContext(Key_defContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Table_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_def(this);
		}
	}

	public final Table_defContext table_def() throws RecognitionException {
		Table_defContext _localctx = new Table_defContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_table_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1126);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
			case 1:
				{
				setState(1124);
				field_def();
				}
				break;
			case 2:
				{
				setState(1125);
				key_def();
				}
				break;
			}
			setState(1135);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1128);
				match(COMMA);
				setState(1131);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,105,_ctx) ) {
				case 1:
					{
					setState(1129);
					field_def();
					}
					break;
				case 2:
					{
					setState(1130);
					key_def();
					}
					break;
				}
				}
				}
				setState(1137);
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

	public static class Key_defContext extends ParserRuleContext {
		public TerminalNode PRIMARY() { return getToken(KVQLParser.PRIMARY, 0); }
		public TerminalNode KEY() { return getToken(KVQLParser.KEY, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Shard_key_defContext shard_key_def() {
			return getRuleContext(Shard_key_defContext.class,0);
		}
		public Id_list_with_sizeContext id_list_with_size() {
			return getRuleContext(Id_list_with_sizeContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(KVQLParser.COMMA, 0); }
		public Key_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_key_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterKey_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitKey_def(this);
		}
	}

	public final Key_defContext key_def() throws RecognitionException {
		Key_defContext _localctx = new Key_defContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_key_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1138);
			match(PRIMARY);
			setState(1139);
			match(KEY);
			setState(1140);
			match(LP);
			setState(1145);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
			case 1:
				{
				setState(1141);
				shard_key_def();
				setState(1143);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1142);
					match(COMMA);
					}
				}

				}
				break;
			}
			setState(1148);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)))) != 0) || _la==ID || _la==BAD_ID) {
				{
				setState(1147);
				id_list_with_size();
				}
			}

			setState(1150);
			match(RP);
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

	public static class Shard_key_defContext extends ParserRuleContext {
		public TerminalNode SHARD() { return getToken(KVQLParser.SHARD, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Id_list_with_sizeContext id_list_with_size() {
			return getRuleContext(Id_list_with_sizeContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Shard_key_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_shard_key_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterShard_key_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitShard_key_def(this);
		}
	}

	public final Shard_key_defContext shard_key_def() throws RecognitionException {
		Shard_key_defContext _localctx = new Shard_key_defContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_shard_key_def);
		try {
			setState(1161);
			switch (_input.LA(1)) {
			case SHARD:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1152);
				match(SHARD);
				setState(1153);
				match(LP);
				setState(1154);
				id_list_with_size();
				setState(1155);
				match(RP);
				}
				}
				break;
			case LP:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1157);
				match(LP);
				setState(1158);
				id_list_with_size();
				 notifyErrorListeners("Missing closing ')'"); 
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

	public static class Id_list_with_sizeContext extends ParserRuleContext {
		public List<Id_with_sizeContext> id_with_size() {
			return getRuleContexts(Id_with_sizeContext.class);
		}
		public Id_with_sizeContext id_with_size(int i) {
			return getRuleContext(Id_with_sizeContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Id_list_with_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_list_with_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_list_with_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_list_with_size(this);
		}
	}

	public final Id_list_with_sizeContext id_list_with_size() throws RecognitionException {
		Id_list_with_sizeContext _localctx = new Id_list_with_sizeContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_id_list_with_size);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1163);
			id_with_size();
			setState(1168);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1164);
					match(COMMA);
					setState(1165);
					id_with_size();
					}
					} 
				}
				setState(1170);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
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

	public static class Id_with_sizeContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Storage_sizeContext storage_size() {
			return getRuleContext(Storage_sizeContext.class,0);
		}
		public Id_with_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_with_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_with_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_with_size(this);
		}
	}

	public final Id_with_sizeContext id_with_size() throws RecognitionException {
		Id_with_sizeContext _localctx = new Id_with_sizeContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_id_with_size);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1171);
			id();
			setState(1173);
			_la = _input.LA(1);
			if (_la==LP) {
				{
				setState(1172);
				storage_size();
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

	public static class Storage_sizeContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Storage_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storage_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterStorage_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitStorage_size(this);
		}
	}

	public final Storage_sizeContext storage_size() throws RecognitionException {
		Storage_sizeContext _localctx = new Storage_sizeContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_storage_size);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1175);
			match(LP);
			setState(1176);
			match(INT);
			setState(1177);
			match(RP);
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

	public static class Ttl_defContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public DurationContext duration() {
			return getRuleContext(DurationContext.class,0);
		}
		public Ttl_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ttl_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTtl_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTtl_def(this);
		}
	}

	public final Ttl_defContext ttl_def() throws RecognitionException {
		Ttl_defContext _localctx = new Ttl_defContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_ttl_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1179);
			match(USING);
			setState(1180);
			match(TTL);
			setState(1181);
			duration();
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

	public static class Alter_table_statementContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(KVQLParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Alter_defContext alter_def() {
			return getRuleContext(Alter_defContext.class,0);
		}
		public Alter_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_table_statement(this);
		}
	}

	public final Alter_table_statementContext alter_table_statement() throws RecognitionException {
		Alter_table_statementContext _localctx = new Alter_table_statementContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_alter_table_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1183);
			match(ALTER);
			setState(1184);
			match(TABLE);
			setState(1185);
			table_name();
			setState(1186);
			alter_def();
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

	public static class Alter_defContext extends ParserRuleContext {
		public Alter_field_statementsContext alter_field_statements() {
			return getRuleContext(Alter_field_statementsContext.class,0);
		}
		public Ttl_defContext ttl_def() {
			return getRuleContext(Ttl_defContext.class,0);
		}
		public Alter_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_def(this);
		}
	}

	public final Alter_defContext alter_def() throws RecognitionException {
		Alter_defContext _localctx = new Alter_defContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_alter_def);
		try {
			setState(1190);
			switch (_input.LA(1)) {
			case LP:
				enterOuterAlt(_localctx, 1);
				{
				setState(1188);
				alter_field_statements();
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1189);
				ttl_def();
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

	public static class Alter_field_statementsContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<Add_field_statementContext> add_field_statement() {
			return getRuleContexts(Add_field_statementContext.class);
		}
		public Add_field_statementContext add_field_statement(int i) {
			return getRuleContext(Add_field_statementContext.class,i);
		}
		public List<Drop_field_statementContext> drop_field_statement() {
			return getRuleContexts(Drop_field_statementContext.class);
		}
		public Drop_field_statementContext drop_field_statement(int i) {
			return getRuleContext(Drop_field_statementContext.class,i);
		}
		public List<Modify_field_statementContext> modify_field_statement() {
			return getRuleContexts(Modify_field_statementContext.class);
		}
		public Modify_field_statementContext modify_field_statement(int i) {
			return getRuleContext(Modify_field_statementContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Alter_field_statementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_field_statements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_field_statements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_field_statements(this);
		}
	}

	public final Alter_field_statementsContext alter_field_statements() throws RecognitionException {
		Alter_field_statementsContext _localctx = new Alter_field_statementsContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_alter_field_statements);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1192);
			match(LP);
			setState(1196);
			switch (_input.LA(1)) {
			case ADD:
				{
				setState(1193);
				add_field_statement();
				}
				break;
			case DROP:
				{
				setState(1194);
				drop_field_statement();
				}
				break;
			case MODIFY:
				{
				setState(1195);
				modify_field_statement();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1206);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1198);
				match(COMMA);
				setState(1202);
				switch (_input.LA(1)) {
				case ADD:
					{
					setState(1199);
					add_field_statement();
					}
					break;
				case DROP:
					{
					setState(1200);
					drop_field_statement();
					}
					break;
				case MODIFY:
					{
					setState(1201);
					modify_field_statement();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1208);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1209);
			match(RP);
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

	public static class Add_field_statementContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public Schema_pathContext schema_path() {
			return getRuleContext(Schema_pathContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Add_field_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_field_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_field_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_field_statement(this);
		}
	}

	public final Add_field_statementContext add_field_statement() throws RecognitionException {
		Add_field_statementContext _localctx = new Add_field_statementContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_add_field_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1211);
			match(ADD);
			setState(1212);
			schema_path();
			setState(1213);
			type_def();
			setState(1215);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==NOT) {
				{
				setState(1214);
				default_def();
				}
			}

			setState(1218);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1217);
				comment();
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

	public static class Drop_field_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public Schema_pathContext schema_path() {
			return getRuleContext(Schema_pathContext.class,0);
		}
		public Drop_field_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_field_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_field_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_field_statement(this);
		}
	}

	public final Drop_field_statementContext drop_field_statement() throws RecognitionException {
		Drop_field_statementContext _localctx = new Drop_field_statementContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_drop_field_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1220);
			match(DROP);
			setState(1221);
			schema_path();
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

	public static class Modify_field_statementContext extends ParserRuleContext {
		public TerminalNode MODIFY() { return getToken(KVQLParser.MODIFY, 0); }
		public Schema_pathContext schema_path() {
			return getRuleContext(Schema_pathContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Modify_field_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_modify_field_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterModify_field_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitModify_field_statement(this);
		}
	}

	public final Modify_field_statementContext modify_field_statement() throws RecognitionException {
		Modify_field_statementContext _localctx = new Modify_field_statementContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_modify_field_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1223);
			match(MODIFY);
			setState(1224);
			schema_path();
			setState(1225);
			type_def();
			setState(1227);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==NOT) {
				{
				setState(1226);
				default_def();
				}
			}

			setState(1230);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1229);
				comment();
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

	public static class Schema_pathContext extends ParserRuleContext {
		public Init_schema_path_stepContext init_schema_path_step() {
			return getRuleContext(Init_schema_path_stepContext.class,0);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public List<Schema_path_stepContext> schema_path_step() {
			return getRuleContexts(Schema_path_stepContext.class);
		}
		public Schema_path_stepContext schema_path_step(int i) {
			return getRuleContext(Schema_path_stepContext.class,i);
		}
		public Schema_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSchema_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSchema_path(this);
		}
	}

	public final Schema_pathContext schema_path() throws RecognitionException {
		Schema_pathContext _localctx = new Schema_pathContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_schema_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1232);
			init_schema_path_step();
			setState(1237);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1233);
				match(DOT);
				setState(1234);
				schema_path_step();
				}
				}
				setState(1239);
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

	public static class Init_schema_path_stepContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public List<TerminalNode> LBRACK() { return getTokens(KVQLParser.LBRACK); }
		public TerminalNode LBRACK(int i) {
			return getToken(KVQLParser.LBRACK, i);
		}
		public List<TerminalNode> RBRACK() { return getTokens(KVQLParser.RBRACK); }
		public TerminalNode RBRACK(int i) {
			return getToken(KVQLParser.RBRACK, i);
		}
		public Init_schema_path_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_init_schema_path_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInit_schema_path_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInit_schema_path_step(this);
		}
	}

	public final Init_schema_path_stepContext init_schema_path_step() throws RecognitionException {
		Init_schema_path_stepContext _localctx = new Init_schema_path_stepContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_init_schema_path_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1240);
			id();
			setState(1245);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LBRACK) {
				{
				{
				setState(1241);
				match(LBRACK);
				setState(1242);
				match(RBRACK);
				}
				}
				setState(1247);
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

	public static class Schema_path_stepContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public List<TerminalNode> LBRACK() { return getTokens(KVQLParser.LBRACK); }
		public TerminalNode LBRACK(int i) {
			return getToken(KVQLParser.LBRACK, i);
		}
		public List<TerminalNode> RBRACK() { return getTokens(KVQLParser.RBRACK); }
		public TerminalNode RBRACK(int i) {
			return getToken(KVQLParser.RBRACK, i);
		}
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Schema_path_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_path_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSchema_path_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSchema_path_step(this);
		}
	}

	public final Schema_path_stepContext schema_path_step() throws RecognitionException {
		Schema_path_stepContext _localctx = new Schema_path_stepContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_schema_path_step);
		int _la;
		try {
			setState(1259);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,124,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1248);
				id();
				setState(1253);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==LBRACK) {
					{
					{
					setState(1249);
					match(LBRACK);
					setState(1250);
					match(RBRACK);
					}
					}
					setState(1255);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1256);
				match(VALUES);
				setState(1257);
				match(LP);
				setState(1258);
				match(RP);
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

	public static class Drop_table_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Drop_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_table_statement(this);
		}
	}

	public final Drop_table_statementContext drop_table_statement() throws RecognitionException {
		Drop_table_statementContext _localctx = new Drop_table_statementContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_drop_table_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1261);
			match(DROP);
			setState(1262);
			match(TABLE);
			setState(1265);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				{
				setState(1263);
				match(IF);
				setState(1264);
				match(EXISTS);
				}
				break;
			}
			setState(1267);
			table_name();
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

	public static class Create_index_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Index_path_listContext index_path_list() {
			return getRuleContext(Index_path_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Create_index_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_index_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_index_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_index_statement(this);
		}
	}

	public final Create_index_statementContext create_index_statement() throws RecognitionException {
		Create_index_statementContext _localctx = new Create_index_statementContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_create_index_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1269);
			match(CREATE);
			setState(1270);
			match(INDEX);
			setState(1274);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,126,_ctx) ) {
			case 1:
				{
				setState(1271);
				match(IF);
				setState(1272);
				match(NOT);
				setState(1273);
				match(EXISTS);
				}
				break;
			}
			setState(1276);
			index_name();
			setState(1277);
			match(ON);
			setState(1278);
			table_name();
			setState(1287);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
			case 1:
				{
				{
				setState(1279);
				match(LP);
				setState(1280);
				index_path_list();
				setState(1281);
				match(RP);
				}
				}
				break;
			case 2:
				{
				{
				setState(1283);
				match(LP);
				setState(1284);
				index_path_list();
				 notifyErrorListeners("Missing closing ')'"); 
				}
				}
				break;
			}
			setState(1290);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1289);
				comment();
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

	public static class Index_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_name(this);
		}
	}

	public final Index_nameContext index_name() throws RecognitionException {
		Index_nameContext _localctx = new Index_nameContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1292);
			id();
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

	public static class Index_path_listContext extends ParserRuleContext {
		public List<Index_pathContext> index_path() {
			return getRuleContexts(Index_pathContext.class);
		}
		public Index_pathContext index_path(int i) {
			return getRuleContext(Index_pathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Index_path_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_path_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_path_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_path_list(this);
		}
	}

	public final Index_path_listContext index_path_list() throws RecognitionException {
		Index_path_listContext _localctx = new Index_path_listContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_index_path_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1294);
			index_path();
			setState(1299);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1295);
				match(COMMA);
				setState(1296);
				index_path();
				}
				}
				setState(1301);
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

	public static class Index_pathContext extends ParserRuleContext {
		public Name_pathContext name_path() {
			return getRuleContext(Name_pathContext.class,0);
		}
		public Path_typeContext path_type() {
			return getRuleContext(Path_typeContext.class,0);
		}
		public Keys_exprContext keys_expr() {
			return getRuleContext(Keys_exprContext.class,0);
		}
		public Values_exprContext values_expr() {
			return getRuleContext(Values_exprContext.class,0);
		}
		public Index_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_path(this);
		}
	}

	public final Index_pathContext index_path() throws RecognitionException {
		Index_pathContext _localctx = new Index_pathContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_index_path);
		int _la;
		try {
			setState(1311);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1302);
				name_path();
				setState(1304);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1303);
					path_type();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1306);
				keys_expr();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1307);
				values_expr();
				setState(1309);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1308);
					path_type();
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

	public static class Keys_exprContext extends ParserRuleContext {
		public Name_pathContext name_path() {
			return getRuleContext(Name_pathContext.class,0);
		}
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode KEYOF() { return getToken(KVQLParser.KEYOF, 0); }
		public Keys_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keys_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterKeys_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitKeys_expr(this);
		}
	}

	public final Keys_exprContext keys_expr() throws RecognitionException {
		Keys_exprContext _localctx = new Keys_exprContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_keys_expr);
		try {
			setState(1329);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,133,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1313);
				name_path();
				setState(1314);
				match(DOT);
				setState(1315);
				match(KEYS);
				setState(1316);
				match(LP);
				setState(1317);
				match(RP);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1319);
				match(KEYOF);
				setState(1320);
				match(LP);
				setState(1321);
				name_path();
				setState(1322);
				match(RP);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1324);
				match(KEYS);
				setState(1325);
				match(LP);
				setState(1326);
				name_path();
				setState(1327);
				match(RP);
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

	public static class Values_exprContext extends ParserRuleContext {
		public List<Name_pathContext> name_path() {
			return getRuleContexts(Name_pathContext.class);
		}
		public Name_pathContext name_path(int i) {
			return getRuleContext(Name_pathContext.class,i);
		}
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public TerminalNode ELEMENTOF() { return getToken(KVQLParser.ELEMENTOF, 0); }
		public Values_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_values_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterValues_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitValues_expr(this);
		}
	}

	public final Values_exprContext values_expr() throws RecognitionException {
		Values_exprContext _localctx = new Values_exprContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_values_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1346);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,134,_ctx) ) {
			case 1:
				{
				{
				setState(1331);
				name_path();
				setState(1332);
				match(DOT);
				setState(1333);
				match(VALUES);
				setState(1334);
				match(LP);
				setState(1335);
				match(RP);
				}
				}
				break;
			case 2:
				{
				{
				setState(1337);
				name_path();
				setState(1338);
				match(LBRACK);
				setState(1339);
				match(RBRACK);
				}
				}
				break;
			case 3:
				{
				{
				setState(1341);
				match(ELEMENTOF);
				setState(1342);
				match(LP);
				setState(1343);
				name_path();
				setState(1344);
				match(RP);
				}
				}
				break;
			}
			setState(1350);
			_la = _input.LA(1);
			if (_la==DOT) {
				{
				setState(1348);
				match(DOT);
				setState(1349);
				name_path();
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

	public static class Path_typeContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public TerminalNode DOUBLE_T() { return getToken(KVQLParser.DOUBLE_T, 0); }
		public TerminalNode STRING_T() { return getToken(KVQLParser.STRING_T, 0); }
		public TerminalNode BOOLEAN_T() { return getToken(KVQLParser.BOOLEAN_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public Path_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPath_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPath_type(this);
		}
	}

	public final Path_typeContext path_type() throws RecognitionException {
		Path_typeContext _localctx = new Path_typeContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_path_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1352);
			match(AS);
			setState(1353);
			_la = _input.LA(1);
			if ( !(((((_la - 103)) & ~0x3f) == 0 && ((1L << (_la - 103)) & ((1L << (BOOLEAN_T - 103)) | (1L << (DOUBLE_T - 103)) | (1L << (INTEGER_T - 103)) | (1L << (LONG_T - 103)) | (1L << (NUMBER_T - 103)) | (1L << (STRING_T - 103)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class Create_text_index_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode FULLTEXT() { return getToken(KVQLParser.FULLTEXT, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Fts_field_listContext fts_field_list() {
			return getRuleContext(Fts_field_listContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Es_propertiesContext es_properties() {
			return getRuleContext(Es_propertiesContext.class,0);
		}
		public TerminalNode OVERRIDE() { return getToken(KVQLParser.OVERRIDE, 0); }
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Create_text_index_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_text_index_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_text_index_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_text_index_statement(this);
		}
	}

	public final Create_text_index_statementContext create_text_index_statement() throws RecognitionException {
		Create_text_index_statementContext _localctx = new Create_text_index_statementContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_create_text_index_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1355);
			match(CREATE);
			setState(1356);
			match(FULLTEXT);
			setState(1357);
			match(INDEX);
			setState(1361);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,136,_ctx) ) {
			case 1:
				{
				setState(1358);
				match(IF);
				setState(1359);
				match(NOT);
				setState(1360);
				match(EXISTS);
				}
				break;
			}
			setState(1363);
			index_name();
			setState(1364);
			match(ON);
			setState(1365);
			table_name();
			setState(1366);
			fts_field_list();
			setState(1368);
			_la = _input.LA(1);
			if (_la==ES_SHARDS || _la==ES_REPLICAS) {
				{
				setState(1367);
				es_properties();
				}
			}

			setState(1371);
			_la = _input.LA(1);
			if (_la==OVERRIDE) {
				{
				setState(1370);
				match(OVERRIDE);
				}
			}

			setState(1374);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1373);
				comment();
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

	public static class Fts_field_listContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Fts_path_listContext fts_path_list() {
			return getRuleContext(Fts_path_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Fts_field_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fts_field_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFts_field_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFts_field_list(this);
		}
	}

	public final Fts_field_listContext fts_field_list() throws RecognitionException {
		Fts_field_listContext _localctx = new Fts_field_listContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_fts_field_list);
		try {
			setState(1384);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,140,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1376);
				match(LP);
				setState(1377);
				fts_path_list();
				setState(1378);
				match(RP);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1380);
				match(LP);
				setState(1381);
				fts_path_list();
				notifyErrorListeners("Missing closing ')'");
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

	public static class Fts_path_listContext extends ParserRuleContext {
		public List<Fts_pathContext> fts_path() {
			return getRuleContexts(Fts_pathContext.class);
		}
		public Fts_pathContext fts_path(int i) {
			return getRuleContext(Fts_pathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Fts_path_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fts_path_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFts_path_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFts_path_list(this);
		}
	}

	public final Fts_path_listContext fts_path_list() throws RecognitionException {
		Fts_path_listContext _localctx = new Fts_path_listContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_fts_path_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1386);
			fts_path();
			setState(1391);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1387);
				match(COMMA);
				setState(1388);
				fts_path();
				}
				}
				setState(1393);
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

	public static class Fts_pathContext extends ParserRuleContext {
		public Index_pathContext index_path() {
			return getRuleContext(Index_pathContext.class,0);
		}
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public Fts_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fts_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFts_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFts_path(this);
		}
	}

	public final Fts_pathContext fts_path() throws RecognitionException {
		Fts_pathContext _localctx = new Fts_pathContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_fts_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1394);
			index_path();
			setState(1396);
			_la = _input.LA(1);
			if (_la==LBRACE) {
				{
				setState(1395);
				jsobject();
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

	public static class Es_propertiesContext extends ParserRuleContext {
		public List<Es_property_assignmentContext> es_property_assignment() {
			return getRuleContexts(Es_property_assignmentContext.class);
		}
		public Es_property_assignmentContext es_property_assignment(int i) {
			return getRuleContext(Es_property_assignmentContext.class,i);
		}
		public Es_propertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_es_properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEs_properties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEs_properties(this);
		}
	}

	public final Es_propertiesContext es_properties() throws RecognitionException {
		Es_propertiesContext _localctx = new Es_propertiesContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_es_properties);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1398);
			es_property_assignment();
			setState(1402);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ES_SHARDS || _la==ES_REPLICAS) {
				{
				{
				setState(1399);
				es_property_assignment();
				}
				}
				setState(1404);
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

	public static class Es_property_assignmentContext extends ParserRuleContext {
		public TerminalNode ES_SHARDS() { return getToken(KVQLParser.ES_SHARDS, 0); }
		public TerminalNode EQ() { return getToken(KVQLParser.EQ, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode ES_REPLICAS() { return getToken(KVQLParser.ES_REPLICAS, 0); }
		public Es_property_assignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_es_property_assignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEs_property_assignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEs_property_assignment(this);
		}
	}

	public final Es_property_assignmentContext es_property_assignment() throws RecognitionException {
		Es_property_assignmentContext _localctx = new Es_property_assignmentContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_es_property_assignment);
		try {
			setState(1411);
			switch (_input.LA(1)) {
			case ES_SHARDS:
				enterOuterAlt(_localctx, 1);
				{
				setState(1405);
				match(ES_SHARDS);
				setState(1406);
				match(EQ);
				setState(1407);
				match(INT);
				}
				break;
			case ES_REPLICAS:
				enterOuterAlt(_localctx, 2);
				{
				setState(1408);
				match(ES_REPLICAS);
				setState(1409);
				match(EQ);
				setState(1410);
				match(INT);
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

	public static class Drop_index_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public TerminalNode OVERRIDE() { return getToken(KVQLParser.OVERRIDE, 0); }
		public Drop_index_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_index_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_index_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_index_statement(this);
		}
	}

	public final Drop_index_statementContext drop_index_statement() throws RecognitionException {
		Drop_index_statementContext _localctx = new Drop_index_statementContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_drop_index_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1413);
			match(DROP);
			setState(1414);
			match(INDEX);
			setState(1417);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
			case 1:
				{
				setState(1415);
				match(IF);
				setState(1416);
				match(EXISTS);
				}
				break;
			}
			setState(1419);
			index_name();
			setState(1420);
			match(ON);
			setState(1421);
			table_name();
			setState(1423);
			_la = _input.LA(1);
			if (_la==OVERRIDE) {
				{
				setState(1422);
				match(OVERRIDE);
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

	public static class Describe_statementContext extends ParserRuleContext {
		public TerminalNode DESCRIBE() { return getToken(KVQLParser.DESCRIBE, 0); }
		public TerminalNode DESC() { return getToken(KVQLParser.DESC, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Schema_path_listContext schema_path_list() {
			return getRuleContext(Schema_path_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Describe_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describe_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDescribe_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDescribe_statement(this);
		}
	}

	public final Describe_statementContext describe_statement() throws RecognitionException {
		Describe_statementContext _localctx = new Describe_statementContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_describe_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1425);
			_la = _input.LA(1);
			if ( !(_la==DESC || _la==DESCRIBE) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1428);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1426);
				match(AS);
				setState(1427);
				match(JSON);
				}
			}

			setState(1447);
			switch (_input.LA(1)) {
			case TABLE:
				{
				setState(1430);
				match(TABLE);
				setState(1431);
				table_name();
				setState(1440);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,148,_ctx) ) {
				case 1:
					{
					{
					setState(1432);
					match(LP);
					setState(1433);
					schema_path_list();
					setState(1434);
					match(RP);
					}
					}
					break;
				case 2:
					{
					{
					setState(1436);
					match(LP);
					setState(1437);
					schema_path_list();
					 notifyErrorListeners("Missing closing ')'")
					             ; 
					}
					}
					break;
				}
				}
				break;
			case INDEX:
				{
				setState(1442);
				match(INDEX);
				setState(1443);
				index_name();
				setState(1444);
				match(ON);
				setState(1445);
				table_name();
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class Schema_path_listContext extends ParserRuleContext {
		public List<Schema_pathContext> schema_path() {
			return getRuleContexts(Schema_pathContext.class);
		}
		public Schema_pathContext schema_path(int i) {
			return getRuleContext(Schema_pathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Schema_path_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_path_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSchema_path_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSchema_path_list(this);
		}
	}

	public final Schema_path_listContext schema_path_list() throws RecognitionException {
		Schema_path_listContext _localctx = new Schema_path_listContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_schema_path_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1449);
			schema_path();
			setState(1454);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1450);
				match(COMMA);
				setState(1451);
				schema_path();
				}
				}
				setState(1456);
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

	public static class Show_statementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(KVQLParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(KVQLParser.TABLES, 0); }
		public TerminalNode USERS() { return getToken(KVQLParser.USERS, 0); }
		public TerminalNode ROLES() { return getToken(KVQLParser.ROLES, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode INDEXES() { return getToken(KVQLParser.INDEXES, 0); }
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public Show_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterShow_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitShow_statement(this);
		}
	}

	public final Show_statementContext show_statement() throws RecognitionException {
		Show_statementContext _localctx = new Show_statementContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_show_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1457);
			match(SHOW);
			setState(1460);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1458);
				match(AS);
				setState(1459);
				match(JSON);
				}
			}

			setState(1474);
			switch (_input.LA(1)) {
			case TABLES:
				{
				setState(1462);
				match(TABLES);
				}
				break;
			case USERS:
				{
				setState(1463);
				match(USERS);
				}
				break;
			case ROLES:
				{
				setState(1464);
				match(ROLES);
				}
				break;
			case USER:
				{
				setState(1465);
				match(USER);
				setState(1466);
				identifier_or_string();
				}
				break;
			case ROLE:
				{
				setState(1467);
				match(ROLE);
				setState(1468);
				id();
				}
				break;
			case INDEXES:
				{
				setState(1469);
				match(INDEXES);
				setState(1470);
				match(ON);
				setState(1471);
				table_name();
				}
				break;
			case TABLE:
				{
				setState(1472);
				match(TABLE);
				setState(1473);
				table_name();
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class Create_user_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Create_user_identified_clauseContext create_user_identified_clause() {
			return getRuleContext(Create_user_identified_clauseContext.class,0);
		}
		public Account_lockContext account_lock() {
			return getRuleContext(Account_lockContext.class,0);
		}
		public TerminalNode ADMIN() { return getToken(KVQLParser.ADMIN, 0); }
		public Create_user_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_user_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_user_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_user_statement(this);
		}
	}

	public final Create_user_statementContext create_user_statement() throws RecognitionException {
		Create_user_statementContext _localctx = new Create_user_statementContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_create_user_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1476);
			match(CREATE);
			setState(1477);
			match(USER);
			setState(1478);
			create_user_identified_clause();
			setState(1480);
			_la = _input.LA(1);
			if (_la==ACCOUNT) {
				{
				setState(1479);
				account_lock();
				}
			}

			setState(1483);
			_la = _input.LA(1);
			if (_la==ADMIN) {
				{
				setState(1482);
				match(ADMIN);
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

	public static class Create_role_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Create_role_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_role_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_role_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_role_statement(this);
		}
	}

	public final Create_role_statementContext create_role_statement() throws RecognitionException {
		Create_role_statementContext _localctx = new Create_role_statementContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_create_role_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1485);
			match(CREATE);
			setState(1486);
			match(ROLE);
			setState(1487);
			id();
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

	public static class Alter_user_statementContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(KVQLParser.ALTER, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public Reset_password_clauseContext reset_password_clause() {
			return getRuleContext(Reset_password_clauseContext.class,0);
		}
		public TerminalNode CLEAR_RETAINED_PASSWORD() { return getToken(KVQLParser.CLEAR_RETAINED_PASSWORD, 0); }
		public TerminalNode PASSWORD_EXPIRE() { return getToken(KVQLParser.PASSWORD_EXPIRE, 0); }
		public Password_lifetimeContext password_lifetime() {
			return getRuleContext(Password_lifetimeContext.class,0);
		}
		public Account_lockContext account_lock() {
			return getRuleContext(Account_lockContext.class,0);
		}
		public Alter_user_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_user_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_user_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_user_statement(this);
		}
	}

	public final Alter_user_statementContext alter_user_statement() throws RecognitionException {
		Alter_user_statementContext _localctx = new Alter_user_statementContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_alter_user_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1489);
			match(ALTER);
			setState(1490);
			match(USER);
			setState(1491);
			identifier_or_string();
			setState(1493);
			_la = _input.LA(1);
			if (_la==IDENTIFIED) {
				{
				setState(1492);
				reset_password_clause();
				}
			}

			setState(1496);
			_la = _input.LA(1);
			if (_la==CLEAR_RETAINED_PASSWORD) {
				{
				setState(1495);
				match(CLEAR_RETAINED_PASSWORD);
				}
			}

			setState(1499);
			_la = _input.LA(1);
			if (_la==PASSWORD_EXPIRE) {
				{
				setState(1498);
				match(PASSWORD_EXPIRE);
				}
			}

			setState(1502);
			_la = _input.LA(1);
			if (_la==PASSWORD) {
				{
				setState(1501);
				password_lifetime();
				}
			}

			setState(1505);
			_la = _input.LA(1);
			if (_la==ACCOUNT) {
				{
				setState(1504);
				account_lock();
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

	public static class Drop_user_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public TerminalNode CASCADE() { return getToken(KVQLParser.CASCADE, 0); }
		public Drop_user_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_user_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_user_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_user_statement(this);
		}
	}

	public final Drop_user_statementContext drop_user_statement() throws RecognitionException {
		Drop_user_statementContext _localctx = new Drop_user_statementContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_drop_user_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1507);
			match(DROP);
			setState(1508);
			match(USER);
			setState(1509);
			identifier_or_string();
			setState(1511);
			_la = _input.LA(1);
			if (_la==CASCADE) {
				{
				setState(1510);
				match(CASCADE);
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

	public static class Drop_role_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Drop_role_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_role_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_role_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_role_statement(this);
		}
	}

	public final Drop_role_statementContext drop_role_statement() throws RecognitionException {
		Drop_role_statementContext _localctx = new Drop_role_statementContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_drop_role_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1513);
			match(DROP);
			setState(1514);
			match(ROLE);
			setState(1515);
			id();
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

	public static class Grant_statementContext extends ParserRuleContext {
		public TerminalNode GRANT() { return getToken(KVQLParser.GRANT, 0); }
		public Grant_rolesContext grant_roles() {
			return getRuleContext(Grant_rolesContext.class,0);
		}
		public Grant_system_privilegesContext grant_system_privileges() {
			return getRuleContext(Grant_system_privilegesContext.class,0);
		}
		public Grant_object_privilegesContext grant_object_privileges() {
			return getRuleContext(Grant_object_privilegesContext.class,0);
		}
		public Grant_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_statement(this);
		}
	}

	public final Grant_statementContext grant_statement() throws RecognitionException {
		Grant_statementContext _localctx = new Grant_statementContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_grant_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1517);
			match(GRANT);
			setState(1521);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,161,_ctx) ) {
			case 1:
				{
				setState(1518);
				grant_roles();
				}
				break;
			case 2:
				{
				setState(1519);
				grant_system_privileges();
				}
				break;
			case 3:
				{
				setState(1520);
				grant_object_privileges();
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

	public static class Revoke_statementContext extends ParserRuleContext {
		public TerminalNode REVOKE() { return getToken(KVQLParser.REVOKE, 0); }
		public Revoke_rolesContext revoke_roles() {
			return getRuleContext(Revoke_rolesContext.class,0);
		}
		public Revoke_system_privilegesContext revoke_system_privileges() {
			return getRuleContext(Revoke_system_privilegesContext.class,0);
		}
		public Revoke_object_privilegesContext revoke_object_privileges() {
			return getRuleContext(Revoke_object_privilegesContext.class,0);
		}
		public Revoke_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_statement(this);
		}
	}

	public final Revoke_statementContext revoke_statement() throws RecognitionException {
		Revoke_statementContext _localctx = new Revoke_statementContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_revoke_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1523);
			match(REVOKE);
			setState(1527);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,162,_ctx) ) {
			case 1:
				{
				setState(1524);
				revoke_roles();
				}
				break;
			case 2:
				{
				setState(1525);
				revoke_system_privileges();
				}
				break;
			case 3:
				{
				setState(1526);
				revoke_object_privileges();
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

	public static class Identifier_or_stringContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public Identifier_or_stringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier_or_string; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIdentifier_or_string(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIdentifier_or_string(this);
		}
	}

	public final Identifier_or_stringContext identifier_or_string() throws RecognitionException {
		Identifier_or_stringContext _localctx = new Identifier_or_stringContext(_ctx, getState());
		enterRule(_localctx, 274, RULE_identifier_or_string);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1531);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
			case BAD_ID:
				{
				setState(1529);
				id();
				}
				break;
			case DSTRING:
			case STRING:
				{
				setState(1530);
				string();
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class Identified_clauseContext extends ParserRuleContext {
		public TerminalNode IDENTIFIED() { return getToken(KVQLParser.IDENTIFIED, 0); }
		public By_passwordContext by_password() {
			return getRuleContext(By_passwordContext.class,0);
		}
		public Identified_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identified_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIdentified_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIdentified_clause(this);
		}
	}

	public final Identified_clauseContext identified_clause() throws RecognitionException {
		Identified_clauseContext _localctx = new Identified_clauseContext(_ctx, getState());
		enterRule(_localctx, 276, RULE_identified_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1533);
			match(IDENTIFIED);
			setState(1534);
			by_password();
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

	public static class Create_user_identified_clauseContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Identified_clauseContext identified_clause() {
			return getRuleContext(Identified_clauseContext.class,0);
		}
		public TerminalNode PASSWORD_EXPIRE() { return getToken(KVQLParser.PASSWORD_EXPIRE, 0); }
		public Password_lifetimeContext password_lifetime() {
			return getRuleContext(Password_lifetimeContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode IDENTIFIED_EXTERNALLY() { return getToken(KVQLParser.IDENTIFIED_EXTERNALLY, 0); }
		public Create_user_identified_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_user_identified_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_user_identified_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_user_identified_clause(this);
		}
	}

	public final Create_user_identified_clauseContext create_user_identified_clause() throws RecognitionException {
		Create_user_identified_clauseContext _localctx = new Create_user_identified_clauseContext(_ctx, getState());
		enterRule(_localctx, 278, RULE_create_user_identified_clause);
		int _la;
		try {
			setState(1547);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
			case BAD_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1536);
				id();
				setState(1537);
				identified_clause();
				setState(1539);
				_la = _input.LA(1);
				if (_la==PASSWORD_EXPIRE) {
					{
					setState(1538);
					match(PASSWORD_EXPIRE);
					}
				}

				setState(1542);
				_la = _input.LA(1);
				if (_la==PASSWORD) {
					{
					setState(1541);
					password_lifetime();
					}
				}

				}
				break;
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1544);
				string();
				setState(1545);
				match(IDENTIFIED_EXTERNALLY);
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

	public static class By_passwordContext extends ParserRuleContext {
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public By_passwordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_by_password; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBy_password(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBy_password(this);
		}
	}

	public final By_passwordContext by_password() throws RecognitionException {
		By_passwordContext _localctx = new By_passwordContext(_ctx, getState());
		enterRule(_localctx, 280, RULE_by_password);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1549);
			match(BY);
			setState(1550);
			string();
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

	public static class Password_lifetimeContext extends ParserRuleContext {
		public TerminalNode PASSWORD() { return getToken(KVQLParser.PASSWORD, 0); }
		public TerminalNode LIFETIME() { return getToken(KVQLParser.LIFETIME, 0); }
		public DurationContext duration() {
			return getRuleContext(DurationContext.class,0);
		}
		public Password_lifetimeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_password_lifetime; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPassword_lifetime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPassword_lifetime(this);
		}
	}

	public final Password_lifetimeContext password_lifetime() throws RecognitionException {
		Password_lifetimeContext _localctx = new Password_lifetimeContext(_ctx, getState());
		enterRule(_localctx, 282, RULE_password_lifetime);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1552);
			match(PASSWORD);
			setState(1553);
			match(LIFETIME);
			setState(1554);
			duration();
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

	public static class Reset_password_clauseContext extends ParserRuleContext {
		public Identified_clauseContext identified_clause() {
			return getRuleContext(Identified_clauseContext.class,0);
		}
		public TerminalNode RETAIN_CURRENT_PASSWORD() { return getToken(KVQLParser.RETAIN_CURRENT_PASSWORD, 0); }
		public Reset_password_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_reset_password_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterReset_password_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitReset_password_clause(this);
		}
	}

	public final Reset_password_clauseContext reset_password_clause() throws RecognitionException {
		Reset_password_clauseContext _localctx = new Reset_password_clauseContext(_ctx, getState());
		enterRule(_localctx, 284, RULE_reset_password_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1556);
			identified_clause();
			setState(1558);
			_la = _input.LA(1);
			if (_la==RETAIN_CURRENT_PASSWORD) {
				{
				setState(1557);
				match(RETAIN_CURRENT_PASSWORD);
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

	public static class Account_lockContext extends ParserRuleContext {
		public TerminalNode ACCOUNT() { return getToken(KVQLParser.ACCOUNT, 0); }
		public TerminalNode LOCK() { return getToken(KVQLParser.LOCK, 0); }
		public TerminalNode UNLOCK() { return getToken(KVQLParser.UNLOCK, 0); }
		public Account_lockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_account_lock; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAccount_lock(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAccount_lock(this);
		}
	}

	public final Account_lockContext account_lock() throws RecognitionException {
		Account_lockContext _localctx = new Account_lockContext(_ctx, getState());
		enterRule(_localctx, 286, RULE_account_lock);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1560);
			match(ACCOUNT);
			setState(1561);
			_la = _input.LA(1);
			if ( !(_la==LOCK || _la==UNLOCK) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class Grant_rolesContext extends ParserRuleContext {
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public Grant_rolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_roles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_roles(this);
		}
	}

	public final Grant_rolesContext grant_roles() throws RecognitionException {
		Grant_rolesContext _localctx = new Grant_rolesContext(_ctx, getState());
		enterRule(_localctx, 288, RULE_grant_roles);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1563);
			id_list();
			setState(1564);
			match(TO);
			setState(1565);
			principal();
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

	public static class Grant_system_privilegesContext extends ParserRuleContext {
		public Sys_priv_listContext sys_priv_list() {
			return getRuleContext(Sys_priv_listContext.class,0);
		}
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Grant_system_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_system_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_system_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_system_privileges(this);
		}
	}

	public final Grant_system_privilegesContext grant_system_privileges() throws RecognitionException {
		Grant_system_privilegesContext _localctx = new Grant_system_privilegesContext(_ctx, getState());
		enterRule(_localctx, 290, RULE_grant_system_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1567);
			sys_priv_list();
			setState(1568);
			match(TO);
			setState(1569);
			id();
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

	public static class Grant_object_privilegesContext extends ParserRuleContext {
		public Obj_priv_listContext obj_priv_list() {
			return getRuleContext(Obj_priv_listContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public ObjectContext object() {
			return getRuleContext(ObjectContext.class,0);
		}
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Grant_object_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_object_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_object_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_object_privileges(this);
		}
	}

	public final Grant_object_privilegesContext grant_object_privileges() throws RecognitionException {
		Grant_object_privilegesContext _localctx = new Grant_object_privilegesContext(_ctx, getState());
		enterRule(_localctx, 292, RULE_grant_object_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1571);
			obj_priv_list();
			setState(1572);
			match(ON);
			setState(1573);
			object();
			setState(1574);
			match(TO);
			setState(1575);
			id();
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

	public static class Revoke_rolesContext extends ParserRuleContext {
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public Revoke_rolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_roles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_roles(this);
		}
	}

	public final Revoke_rolesContext revoke_roles() throws RecognitionException {
		Revoke_rolesContext _localctx = new Revoke_rolesContext(_ctx, getState());
		enterRule(_localctx, 294, RULE_revoke_roles);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1577);
			id_list();
			setState(1578);
			match(FROM);
			setState(1579);
			principal();
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

	public static class Revoke_system_privilegesContext extends ParserRuleContext {
		public Sys_priv_listContext sys_priv_list() {
			return getRuleContext(Sys_priv_listContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Revoke_system_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_system_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_system_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_system_privileges(this);
		}
	}

	public final Revoke_system_privilegesContext revoke_system_privileges() throws RecognitionException {
		Revoke_system_privilegesContext _localctx = new Revoke_system_privilegesContext(_ctx, getState());
		enterRule(_localctx, 296, RULE_revoke_system_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1581);
			sys_priv_list();
			setState(1582);
			match(FROM);
			setState(1583);
			id();
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

	public static class Revoke_object_privilegesContext extends ParserRuleContext {
		public Obj_priv_listContext obj_priv_list() {
			return getRuleContext(Obj_priv_listContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public ObjectContext object() {
			return getRuleContext(ObjectContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Revoke_object_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_object_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_object_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_object_privileges(this);
		}
	}

	public final Revoke_object_privilegesContext revoke_object_privileges() throws RecognitionException {
		Revoke_object_privilegesContext _localctx = new Revoke_object_privilegesContext(_ctx, getState());
		enterRule(_localctx, 298, RULE_revoke_object_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1585);
			obj_priv_list();
			setState(1586);
			match(ON);
			setState(1587);
			object();
			setState(1588);
			match(FROM);
			setState(1589);
			id();
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

	public static class PrincipalContext extends ParserRuleContext {
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public PrincipalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_principal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPrincipal(this);
		}
	}

	public final PrincipalContext principal() throws RecognitionException {
		PrincipalContext _localctx = new PrincipalContext(_ctx, getState());
		enterRule(_localctx, 300, RULE_principal);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1595);
			switch (_input.LA(1)) {
			case USER:
				{
				setState(1591);
				match(USER);
				setState(1592);
				identifier_or_string();
				}
				break;
			case ROLE:
				{
				setState(1593);
				match(ROLE);
				setState(1594);
				id();
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class Sys_priv_listContext extends ParserRuleContext {
		public List<Priv_itemContext> priv_item() {
			return getRuleContexts(Priv_itemContext.class);
		}
		public Priv_itemContext priv_item(int i) {
			return getRuleContext(Priv_itemContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Sys_priv_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sys_priv_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSys_priv_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSys_priv_list(this);
		}
	}

	public final Sys_priv_listContext sys_priv_list() throws RecognitionException {
		Sys_priv_listContext _localctx = new Sys_priv_listContext(_ctx, getState());
		enterRule(_localctx, 302, RULE_sys_priv_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1597);
			priv_item();
			setState(1602);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1598);
				match(COMMA);
				setState(1599);
				priv_item();
				}
				}
				setState(1604);
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

	public static class Priv_itemContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode ALL_PRIVILEGES() { return getToken(KVQLParser.ALL_PRIVILEGES, 0); }
		public Priv_itemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_priv_item; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPriv_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPriv_item(this);
		}
	}

	public final Priv_itemContext priv_item() throws RecognitionException {
		Priv_itemContext _localctx = new Priv_itemContext(_ctx, getState());
		enterRule(_localctx, 304, RULE_priv_item);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1607);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
			case BAD_ID:
				{
				setState(1605);
				id();
				}
				break;
			case ALL_PRIVILEGES:
				{
				setState(1606);
				match(ALL_PRIVILEGES);
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	public static class Obj_priv_listContext extends ParserRuleContext {
		public List<Priv_itemContext> priv_item() {
			return getRuleContexts(Priv_itemContext.class);
		}
		public Priv_itemContext priv_item(int i) {
			return getRuleContext(Priv_itemContext.class,i);
		}
		public List<TerminalNode> ALL() { return getTokens(KVQLParser.ALL); }
		public TerminalNode ALL(int i) {
			return getToken(KVQLParser.ALL, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Obj_priv_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_obj_priv_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterObj_priv_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitObj_priv_list(this);
		}
	}

	public final Obj_priv_listContext obj_priv_list() throws RecognitionException {
		Obj_priv_listContext _localctx = new Obj_priv_listContext(_ctx, getState());
		enterRule(_localctx, 306, RULE_obj_priv_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1611);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
			case 1:
				{
				setState(1609);
				priv_item();
				}
				break;
			case 2:
				{
				setState(1610);
				match(ALL);
				}
				break;
			}
			setState(1620);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1613);
				match(COMMA);
				setState(1616);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
				case 1:
					{
					setState(1614);
					priv_item();
					}
					break;
				case 2:
					{
					setState(1615);
					match(ALL);
					}
					break;
				}
				}
				}
				setState(1622);
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

	public static class ObjectContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public ObjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_object; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitObject(this);
		}
	}

	public final ObjectContext object() throws RecognitionException {
		ObjectContext _localctx = new ObjectContext(_ctx, getState());
		enterRule(_localctx, 308, RULE_object);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1623);
			table_name();
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

	public static class Json_textContext extends ParserRuleContext {
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public JsarrayContext jsarray() {
			return getRuleContext(JsarrayContext.class,0);
		}
		public Json_textContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_text; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_text(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_text(this);
		}
	}

	public final Json_textContext json_text() throws RecognitionException {
		Json_textContext _localctx = new Json_textContext(_ctx, getState());
		enterRule(_localctx, 310, RULE_json_text);
		try {
			setState(1627);
			switch (_input.LA(1)) {
			case LBRACE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1625);
				jsobject();
				}
				break;
			case LBRACK:
				enterOuterAlt(_localctx, 2);
				{
				setState(1626);
				jsarray();
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

	public static class JsobjectContext extends ParserRuleContext {
		public JsobjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsobject; }
	 
		public JsobjectContext() { }
		public void copyFrom(JsobjectContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class JsonObjectContext extends JsobjectContext {
		public TerminalNode LBRACE() { return getToken(KVQLParser.LBRACE, 0); }
		public List<JspairContext> jspair() {
			return getRuleContexts(JspairContext.class);
		}
		public JspairContext jspair(int i) {
			return getRuleContext(JspairContext.class,i);
		}
		public TerminalNode RBRACE() { return getToken(KVQLParser.RBRACE, 0); }
		public JsonObjectContext(JsobjectContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonObject(this);
		}
	}
	public static class EmptyJsonObjectContext extends JsobjectContext {
		public TerminalNode LBRACE() { return getToken(KVQLParser.LBRACE, 0); }
		public TerminalNode RBRACE() { return getToken(KVQLParser.RBRACE, 0); }
		public EmptyJsonObjectContext(JsobjectContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEmptyJsonObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEmptyJsonObject(this);
		}
	}

	public final JsobjectContext jsobject() throws RecognitionException {
		JsobjectContext _localctx = new JsobjectContext(_ctx, getState());
		enterRule(_localctx, 312, RULE_jsobject);
		int _la;
		try {
			setState(1642);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
			case 1:
				_localctx = new JsonObjectContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1629);
				match(LBRACE);
				setState(1630);
				jspair();
				setState(1635);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1631);
					match(COMMA);
					setState(1632);
					jspair();
					}
					}
					setState(1637);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1638);
				match(RBRACE);
				}
				break;
			case 2:
				_localctx = new EmptyJsonObjectContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1640);
				match(LBRACE);
				setState(1641);
				match(RBRACE);
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

	public static class JsarrayContext extends ParserRuleContext {
		public JsarrayContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsarray; }
	 
		public JsarrayContext() { }
		public void copyFrom(JsarrayContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class EmptyJsonArrayContext extends JsarrayContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public EmptyJsonArrayContext(JsarrayContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEmptyJsonArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEmptyJsonArray(this);
		}
	}
	public static class ArrayOfJsonValuesContext extends JsarrayContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public List<JsvalueContext> jsvalue() {
			return getRuleContexts(JsvalueContext.class);
		}
		public JsvalueContext jsvalue(int i) {
			return getRuleContext(JsvalueContext.class,i);
		}
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public ArrayOfJsonValuesContext(JsarrayContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArrayOfJsonValues(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArrayOfJsonValues(this);
		}
	}

	public final JsarrayContext jsarray() throws RecognitionException {
		JsarrayContext _localctx = new JsarrayContext(_ctx, getState());
		enterRule(_localctx, 314, RULE_jsarray);
		int _la;
		try {
			setState(1657);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,178,_ctx) ) {
			case 1:
				_localctx = new ArrayOfJsonValuesContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1644);
				match(LBRACK);
				setState(1645);
				jsvalue();
				setState(1650);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1646);
					match(COMMA);
					setState(1647);
					jsvalue();
					}
					}
					setState(1652);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1653);
				match(RBRACK);
				}
				break;
			case 2:
				_localctx = new EmptyJsonArrayContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1655);
				match(LBRACK);
				setState(1656);
				match(RBRACK);
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

	public static class JspairContext extends ParserRuleContext {
		public JspairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jspair; }
	 
		public JspairContext() { }
		public void copyFrom(JspairContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class JsonPairContext extends JspairContext {
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public JsvalueContext jsvalue() {
			return getRuleContext(JsvalueContext.class,0);
		}
		public JsonPairContext(JspairContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonPair(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonPair(this);
		}
	}

	public final JspairContext jspair() throws RecognitionException {
		JspairContext _localctx = new JspairContext(_ctx, getState());
		enterRule(_localctx, 316, RULE_jspair);
		try {
			_localctx = new JsonPairContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(1659);
			match(DSTRING);
			setState(1660);
			match(COLON);
			setState(1661);
			jsvalue();
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

	public static class JsvalueContext extends ParserRuleContext {
		public JsvalueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsvalue; }
	 
		public JsvalueContext() { }
		public void copyFrom(JsvalueContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class JsonAtomContext extends JsvalueContext {
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public JsonAtomContext(JsvalueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonAtom(this);
		}
	}
	public static class JsonArrayValueContext extends JsvalueContext {
		public JsarrayContext jsarray() {
			return getRuleContext(JsarrayContext.class,0);
		}
		public JsonArrayValueContext(JsvalueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonArrayValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonArrayValue(this);
		}
	}
	public static class JsonObjectValueContext extends JsvalueContext {
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public JsonObjectValueContext(JsvalueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonObjectValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonObjectValue(this);
		}
	}

	public final JsvalueContext jsvalue() throws RecognitionException {
		JsvalueContext _localctx = new JsvalueContext(_ctx, getState());
		enterRule(_localctx, 318, RULE_jsvalue);
		try {
			setState(1670);
			switch (_input.LA(1)) {
			case LBRACE:
				_localctx = new JsonObjectValueContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1663);
				jsobject();
				}
				break;
			case LBRACK:
				_localctx = new JsonArrayValueContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1664);
				jsarray();
				}
				break;
			case DSTRING:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1665);
				match(DSTRING);
				}
				break;
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1666);
				number();
				}
				break;
			case TRUE:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1667);
				match(TRUE);
				}
				break;
			case FALSE:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1668);
				match(FALSE);
				}
				break;
			case NULL:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1669);
				match(NULL);
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

	public static class CommentContext extends ParserRuleContext {
		public TerminalNode COMMENT() { return getToken(KVQLParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public CommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitComment(this);
		}
	}

	public final CommentContext comment() throws RecognitionException {
		CommentContext _localctx = new CommentContext(_ctx, getState());
		enterRule(_localctx, 320, RULE_comment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1672);
			match(COMMENT);
			setState(1673);
			string();
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

	public static class DurationContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public Time_unitContext time_unit() {
			return getRuleContext(Time_unitContext.class,0);
		}
		public DurationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_duration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDuration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDuration(this);
		}
	}

	public final DurationContext duration() throws RecognitionException {
		DurationContext _localctx = new DurationContext(_ctx, getState());
		enterRule(_localctx, 322, RULE_duration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1675);
			match(INT);
			setState(1676);
			time_unit();
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

	public static class Time_unitContext extends ParserRuleContext {
		public TerminalNode SECONDS() { return getToken(KVQLParser.SECONDS, 0); }
		public TerminalNode MINUTES() { return getToken(KVQLParser.MINUTES, 0); }
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public Time_unitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_time_unit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTime_unit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTime_unit(this);
		}
	}

	public final Time_unitContext time_unit() throws RecognitionException {
		Time_unitContext _localctx = new Time_unitContext(_ctx, getState());
		enterRule(_localctx, 324, RULE_time_unit);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1678);
			_la = _input.LA(1);
			if ( !(((((_la - 19)) & ~0x3f) == 0 && ((1L << (_la - 19)) & ((1L << (DAYS - 19)) | (1L << (HOURS - 19)) | (1L << (MINUTES - 19)) | (1L << (SECONDS - 19)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class NumberContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode FLOAT() { return getToken(KVQLParser.FLOAT, 0); }
		public TerminalNode NUMBER() { return getToken(KVQLParser.NUMBER, 0); }
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNumber(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNumber(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 326, RULE_number);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1681);
			_la = _input.LA(1);
			if (_la==MINUS) {
				{
				setState(1680);
				match(MINUS);
				}
			}

			setState(1683);
			_la = _input.LA(1);
			if ( !(((((_la - 150)) & ~0x3f) == 0 && ((1L << (_la - 150)) & ((1L << (INT - 150)) | (1L << (FLOAT - 150)) | (1L << (NUMBER - 150)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class StringContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(KVQLParser.STRING, 0); }
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitString(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 328, RULE_string);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1685);
			_la = _input.LA(1);
			if ( !(_la==DSTRING || _la==STRING) ) {
			_errHandler.recoverInline(this);
			} else {
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

	public static class Id_listContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Id_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_list(this);
		}
	}

	public final Id_listContext id_list() throws RecognitionException {
		Id_listContext _localctx = new Id_listContext(_ctx, getState());
		enterRule(_localctx, 330, RULE_id_list);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1687);
			id();
			setState(1692);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,181,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1688);
					match(COMMA);
					setState(1689);
					id();
					}
					} 
				}
				setState(1694);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,181,_ctx);
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

	public static class IdContext extends ParserRuleContext {
		public TerminalNode ACCOUNT() { return getToken(KVQLParser.ACCOUNT, 0); }
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public TerminalNode ADMIN() { return getToken(KVQLParser.ADMIN, 0); }
		public TerminalNode ALL() { return getToken(KVQLParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(KVQLParser.ALTER, 0); }
		public TerminalNode ANCESTORS() { return getToken(KVQLParser.ANCESTORS, 0); }
		public TerminalNode AND() { return getToken(KVQLParser.AND, 0); }
		public TerminalNode ANY_T() { return getToken(KVQLParser.ANY_T, 0); }
		public TerminalNode ANYATOMIC_T() { return getToken(KVQLParser.ANYATOMIC_T, 0); }
		public TerminalNode ANYJSONATOMIC_T() { return getToken(KVQLParser.ANYJSONATOMIC_T, 0); }
		public TerminalNode ANYRECORD_T() { return getToken(KVQLParser.ANYRECORD_T, 0); }
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode ASC() { return getToken(KVQLParser.ASC, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public TerminalNode CASE() { return getToken(KVQLParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(KVQLParser.CAST, 0); }
		public TerminalNode COMMENT() { return getToken(KVQLParser.COMMENT, 0); }
		public TerminalNode COUNT() { return getToken(KVQLParser.COUNT, 0); }
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public TerminalNode DECLARE() { return getToken(KVQLParser.DECLARE, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public TerminalNode DESC() { return getToken(KVQLParser.DESC, 0); }
		public TerminalNode DESCENDANTS() { return getToken(KVQLParser.DESCENDANTS, 0); }
		public TerminalNode DESCRIBE() { return getToken(KVQLParser.DESCRIBE, 0); }
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode ELEMENTOF() { return getToken(KVQLParser.ELEMENTOF, 0); }
		public TerminalNode ELSE() { return getToken(KVQLParser.ELSE, 0); }
		public TerminalNode END() { return getToken(KVQLParser.END, 0); }
		public TerminalNode ES_SHARDS() { return getToken(KVQLParser.ES_SHARDS, 0); }
		public TerminalNode ES_REPLICAS() { return getToken(KVQLParser.ES_REPLICAS, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public TerminalNode EXTRACT() { return getToken(KVQLParser.EXTRACT, 0); }
		public TerminalNode FIRST() { return getToken(KVQLParser.FIRST, 0); }
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public TerminalNode FULLTEXT() { return getToken(KVQLParser.FULLTEXT, 0); }
		public TerminalNode GRANT() { return getToken(KVQLParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(KVQLParser.GROUP, 0); }
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode IDENTIFIED() { return getToken(KVQLParser.IDENTIFIED, 0); }
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(KVQLParser.INDEXES, 0); }
		public TerminalNode IS() { return getToken(KVQLParser.IS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public TerminalNode KEY() { return getToken(KVQLParser.KEY, 0); }
		public TerminalNode KEYOF() { return getToken(KVQLParser.KEYOF, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public TerminalNode LIFETIME() { return getToken(KVQLParser.LIFETIME, 0); }
		public TerminalNode LAST() { return getToken(KVQLParser.LAST, 0); }
		public TerminalNode LIMIT() { return getToken(KVQLParser.LIMIT, 0); }
		public TerminalNode LOCK() { return getToken(KVQLParser.LOCK, 0); }
		public TerminalNode MINUTES() { return getToken(KVQLParser.MINUTES, 0); }
		public TerminalNode MODIFY() { return getToken(KVQLParser.MODIFY, 0); }
		public TerminalNode NESTED() { return getToken(KVQLParser.NESTED, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode NULLS() { return getToken(KVQLParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(KVQLParser.OF, 0); }
		public TerminalNode OFFSET() { return getToken(KVQLParser.OFFSET, 0); }
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public TerminalNode OR() { return getToken(KVQLParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(KVQLParser.ORDER, 0); }
		public TerminalNode OVERRIDE() { return getToken(KVQLParser.OVERRIDE, 0); }
		public TerminalNode PASSWORD() { return getToken(KVQLParser.PASSWORD, 0); }
		public TerminalNode PRIMARY() { return getToken(KVQLParser.PRIMARY, 0); }
		public TerminalNode PUT() { return getToken(KVQLParser.PUT, 0); }
		public TerminalNode REMOVE() { return getToken(KVQLParser.REMOVE, 0); }
		public TerminalNode RETURNING() { return getToken(KVQLParser.RETURNING, 0); }
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(KVQLParser.ROLES, 0); }
		public TerminalNode REVOKE() { return getToken(KVQLParser.REVOKE, 0); }
		public TerminalNode SECONDS() { return getToken(KVQLParser.SECONDS, 0); }
		public TerminalNode SELECT() { return getToken(KVQLParser.SELECT, 0); }
		public TerminalNode SEQ_TRANSFORM() { return getToken(KVQLParser.SEQ_TRANSFORM, 0); }
		public TerminalNode SET() { return getToken(KVQLParser.SET, 0); }
		public TerminalNode SHARD() { return getToken(KVQLParser.SHARD, 0); }
		public TerminalNode SHOW() { return getToken(KVQLParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode TABLES() { return getToken(KVQLParser.TABLES, 0); }
		public TerminalNode THEN() { return getToken(KVQLParser.THEN, 0); }
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public TerminalNode TYPE() { return getToken(KVQLParser.TYPE, 0); }
		public TerminalNode UNLOCK() { return getToken(KVQLParser.UNLOCK, 0); }
		public TerminalNode UPDATE() { return getToken(KVQLParser.UPDATE, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public TerminalNode USERS() { return getToken(KVQLParser.USERS, 0); }
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public TerminalNode WHEN() { return getToken(KVQLParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public TerminalNode ARRAY_T() { return getToken(KVQLParser.ARRAY_T, 0); }
		public TerminalNode BINARY_T() { return getToken(KVQLParser.BINARY_T, 0); }
		public TerminalNode BOOLEAN_T() { return getToken(KVQLParser.BOOLEAN_T, 0); }
		public TerminalNode DOUBLE_T() { return getToken(KVQLParser.DOUBLE_T, 0); }
		public TerminalNode ENUM_T() { return getToken(KVQLParser.ENUM_T, 0); }
		public TerminalNode FLOAT_T() { return getToken(KVQLParser.FLOAT_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode MAP_T() { return getToken(KVQLParser.MAP_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public TerminalNode RECORD_T() { return getToken(KVQLParser.RECORD_T, 0); }
		public TerminalNode STRING_T() { return getToken(KVQLParser.STRING_T, 0); }
		public TerminalNode TIMESTAMP_T() { return getToken(KVQLParser.TIMESTAMP_T, 0); }
		public TerminalNode SCALAR_T() { return getToken(KVQLParser.SCALAR_T, 0); }
		public TerminalNode ID() { return getToken(KVQLParser.ID, 0); }
		public TerminalNode BAD_ID() { return getToken(KVQLParser.BAD_ID, 0); }
		public IdContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId(this);
		}
	}

	public final IdContext id() throws RecognitionException {
		IdContext _localctx = new IdContext(_ctx, getState());
		enterRule(_localctx, 332, RULE_id);
		int _la;
		try {
			setState(1698);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case BY:
			case CASE:
			case CAST:
			case COMMENT:
			case COUNT:
			case CREATE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DROP:
			case ELEMENTOF:
			case ELSE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIRST:
			case FROM:
			case FULLTEXT:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IF:
			case INDEX:
			case INDEXES:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCK:
			case MINUTES:
			case MODIFY:
			case NESTED:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PRIMARY:
			case PUT:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNLOCK:
			case UPDATE:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1695);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ACCOUNT) | (1L << ADD) | (1L << ADMIN) | (1L << ALL) | (1L << ALTER) | (1L << ANCESTORS) | (1L << AND) | (1L << AS) | (1L << ASC) | (1L << BY) | (1L << CASE) | (1L << CAST) | (1L << COMMENT) | (1L << COUNT) | (1L << CREATE) | (1L << DAYS) | (1L << DECLARE) | (1L << DEFAULT) | (1L << DESC) | (1L << DESCENDANTS) | (1L << DESCRIBE) | (1L << DROP) | (1L << ELEMENTOF) | (1L << ELSE) | (1L << END) | (1L << ES_SHARDS) | (1L << ES_REPLICAS) | (1L << EXISTS) | (1L << EXTRACT) | (1L << FIRST) | (1L << FROM) | (1L << FULLTEXT) | (1L << GRANT) | (1L << GROUP) | (1L << HOURS) | (1L << IDENTIFIED) | (1L << IF) | (1L << INDEX) | (1L << INDEXES) | (1L << IS) | (1L << JSON) | (1L << KEY) | (1L << KEYOF) | (1L << KEYS) | (1L << LAST) | (1L << LIFETIME) | (1L << LIMIT) | (1L << LOCK) | (1L << MINUTES) | (1L << MODIFY) | (1L << NESTED) | (1L << NOT) | (1L << NULLS) | (1L << OFFSET) | (1L << OF) | (1L << ON) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (OVERRIDE - 64)) | (1L << (PASSWORD - 64)) | (1L << (PRIMARY - 64)) | (1L << (PUT - 64)) | (1L << (REMOVE - 64)) | (1L << (RETURNING - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)) | (1L << (ROLES - 64)) | (1L << (SECONDS - 64)) | (1L << (SELECT - 64)) | (1L << (SEQ_TRANSFORM - 64)) | (1L << (SET - 64)) | (1L << (SHARD - 64)) | (1L << (SHOW - 64)) | (1L << (TABLE - 64)) | (1L << (TABLES - 64)) | (1L << (THEN - 64)) | (1L << (TO - 64)) | (1L << (TTL - 64)) | (1L << (TYPE - 64)) | (1L << (UNLOCK - 64)) | (1L << (UPDATE - 64)) | (1L << (USER - 64)) | (1L << (USERS - 64)) | (1L << (USING - 64)) | (1L << (VALUES - 64)) | (1L << (WHEN - 64)) | (1L << (WHERE - 64)) | (1L << (ARRAY_T - 64)) | (1L << (BINARY_T - 64)) | (1L << (BOOLEAN_T - 64)) | (1L << (DOUBLE_T - 64)) | (1L << (ENUM_T - 64)) | (1L << (FLOAT_T - 64)) | (1L << (INTEGER_T - 64)) | (1L << (LONG_T - 64)) | (1L << (MAP_T - 64)) | (1L << (NUMBER_T - 64)) | (1L << (RECORD_T - 64)) | (1L << (STRING_T - 64)) | (1L << (TIMESTAMP_T - 64)) | (1L << (ANY_T - 64)) | (1L << (ANYATOMIC_T - 64)) | (1L << (ANYJSONATOMIC_T - 64)) | (1L << (ANYRECORD_T - 64)) | (1L << (SCALAR_T - 64)))) != 0) || _la==ID) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			case BAD_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(1696);
				match(BAD_ID);

				        notifyErrorListeners("Identifiers must start with a letter: " + _input.getText(_localctx.start, _input.LT(-1)));
				     
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

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 26:
			return or_expr_sempred((Or_exprContext)_localctx, predIndex);
		case 27:
			return and_expr_sempred((And_exprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean or_expr_sempred(Or_exprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean and_expr_sempred(And_exprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u00a4\u06a7\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
		"k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4"+
		"w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080"+
		"\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084\t\u0084\4\u0085"+
		"\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087\4\u0088\t\u0088\4\u0089\t\u0089"+
		"\4\u008a\t\u008a\4\u008b\t\u008b\4\u008c\t\u008c\4\u008d\t\u008d\4\u008e"+
		"\t\u008e\4\u008f\t\u008f\4\u0090\t\u0090\4\u0091\t\u0091\4\u0092\t\u0092"+
		"\4\u0093\t\u0093\4\u0094\t\u0094\4\u0095\t\u0095\4\u0096\t\u0096\4\u0097"+
		"\t\u0097\4\u0098\t\u0098\4\u0099\t\u0099\4\u009a\t\u009a\4\u009b\t\u009b"+
		"\4\u009c\t\u009c\4\u009d\t\u009d\4\u009e\t\u009e\4\u009f\t\u009f\4\u00a0"+
		"\t\u00a0\4\u00a1\t\u00a1\4\u00a2\t\u00a2\4\u00a3\t\u00a3\4\u00a4\t\u00a4"+
		"\4\u00a5\t\u00a5\4\u00a6\t\u00a6\4\u00a7\t\u00a7\4\u00a8\t\u00a8\3\2\3"+
		"\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\5\3\u0165\n\3\3\4\5\4\u0168\n\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\7"+
		"\5\u0172\n\5\f\5\16\5\u0175\13\5\3\6\3\6\3\6\3\7\3\7\3\7\3\b\3\b\3\t\3"+
		"\t\3\t\5\t\u0182\n\t\3\t\5\t\u0185\n\t\3\t\5\t\u0188\n\t\3\t\5\t\u018b"+
		"\n\t\3\t\5\t\u018e\n\t\3\n\3\n\3\n\5\n\u0193\n\n\3\n\3\n\3\n\5\n\u0198"+
		"\n\n\3\n\3\n\7\n\u019c\n\n\f\n\16\n\u019f\13\n\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\5\13\u01aa\n\13\3\13\3\13\3\13\3\13\3\13\5\13\u01b1"+
		"\n\13\3\13\3\13\3\f\3\f\3\f\7\f\u01b8\n\f\f\f\16\f\u01bb\13\f\3\r\3\r"+
		"\3\r\7\r\u01c0\n\r\f\r\16\r\u01c3\13\r\3\16\3\16\3\16\5\16\u01c8\n\16"+
		"\3\17\3\17\5\17\u01cc\n\17\3\17\5\17\u01cf\n\17\3\17\5\17\u01d2\n\17\3"+
		"\20\5\20\u01d5\n\20\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\22\3\23\5\23"+
		"\u01e0\n\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\7\23\u01e9\n\23\f\23\16"+
		"\23\u01ec\13\23\5\23\u01ee\n\23\3\24\3\24\7\24\u01f2\n\24\f\24\16\24\u01f5"+
		"\13\24\3\24\3\24\3\25\3\25\3\25\3\25\7\25\u01fd\n\25\f\25\16\25\u0200"+
		"\13\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25"+
		"\3\25\3\25\3\25\3\25\3\25\5\25\u0214\n\25\3\25\5\25\u0217\n\25\3\26\3"+
		"\26\5\26\u021b\n\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u0225"+
		"\n\27\f\27\16\27\u0228\13\27\3\30\5\30\u022b\n\30\3\30\3\30\5\30\u022f"+
		"\n\30\3\31\3\31\3\31\3\31\3\31\7\31\u0236\n\31\f\31\16\31\u0239\13\31"+
		"\3\32\3\32\3\32\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\7\34\u0247"+
		"\n\34\f\34\16\34\u024a\13\34\3\35\3\35\3\35\3\35\3\35\3\35\7\35\u0252"+
		"\n\35\f\35\16\35\u0255\13\35\3\36\5\36\u0258\n\36\3\36\3\36\3\37\3\37"+
		"\3\37\5\37\u025f\n\37\3\37\5\37\u0262\n\37\3 \3 \3 \5 \u0267\n \3!\3!"+
		"\3!\3\"\3\"\3\"\5\"\u026f\n\"\3\"\3\"\5\"\u0273\n\"\3\"\3\"\5\"\u0277"+
		"\n\"\3\"\3\"\3\"\5\"\u027c\n\"\3\"\7\"\u027f\n\"\f\"\16\"\u0282\13\"\3"+
		"\"\3\"\3#\3#\3#\5#\u0289\n#\3#\3#\5#\u028d\n#\3$\3$\3%\3%\3%\3%\3%\3%"+
		"\5%\u0297\n%\3&\3&\3&\7&\u029c\n&\f&\16&\u029f\13&\3\'\3\'\3\'\7\'\u02a4"+
		"\n\'\f\'\16\'\u02a7\13\'\3(\3(\3(\5(\u02ac\n(\3)\3)\3)\7)\u02b1\n)\f)"+
		"\16)\u02b4\13)\3*\3*\3*\5*\u02b9\n*\3+\3+\3+\3+\3+\5+\u02c0\n+\3,\3,\3"+
		",\5,\u02c5\n,\3,\3,\3-\3-\5-\u02cb\n-\3.\3.\5.\u02cf\n.\3.\3.\5.\u02d3"+
		"\n.\3.\3.\3/\3/\5/\u02d9\n/\3/\3/\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3"+
		"\60\3\60\3\60\3\60\3\60\5\60\u02e9\n\60\3\61\3\61\3\61\5\61\u02ee\n\61"+
		"\3\62\3\62\3\62\3\62\3\62\5\62\u02f5\n\62\3\63\3\63\5\63\u02f9\n\63\3"+
		"\64\3\64\5\64\u02fd\n\64\3\64\3\64\7\64\u0301\n\64\f\64\16\64\u0304\13"+
		"\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\3\65\3\65\3\65\3\65\7\65\u0311"+
		"\n\65\f\65\16\65\u0314\13\65\3\65\3\65\3\65\3\65\5\65\u031a\n\65\3\66"+
		"\3\66\3\66\3\66\3\66\3\66\3\66\3\67\3\67\38\38\38\38\38\78\u032a\n8\f"+
		"8\168\u032d\138\58\u032f\n8\38\38\39\39\39\39\39\3:\3:\3:\3:\3:\3:\3:"+
		"\3:\3:\3:\7:\u0342\n:\f:\16:\u0345\13:\3:\3:\5:\u0349\n:\3:\3:\3;\3;\3"+
		";\3;\3;\3;\3;\3<\3<\3<\3<\3=\3=\3=\3=\3=\3=\3=\3>\5>\u0360\n>\3>\3>\3"+
		">\5>\u0365\n>\3>\5>\u0368\n>\3>\3>\3>\7>\u036d\n>\f>\16>\u0370\13>\3>"+
		"\3>\3>\5>\u0375\n>\3?\3?\3?\3@\3@\3@\3@\3@\5@\u037f\n@\7@\u0381\n@\f@"+
		"\16@\u0384\13@\3@\3@\3@\3@\3@\5@\u038b\n@\7@\u038d\n@\f@\16@\u0390\13"+
		"@\3@\3@\3@\3@\3@\5@\u0397\n@\7@\u0399\n@\f@\16@\u039c\13@\3@\3@\3@\3@"+
		"\7@\u03a2\n@\f@\16@\u03a5\13@\3@\3@\3@\3@\3@\7@\u03ac\n@\f@\16@\u03af"+
		"\13@\5@\u03b1\n@\3A\3A\3A\3A\3B\3B\5B\u03b9\nB\3B\3B\3C\3C\3C\3D\3D\3"+
		"E\3E\3E\3E\3E\3E\5E\u03c8\nE\3F\3F\3G\3G\3H\3H\5H\u03d0\nH\3I\3I\3I\3"+
		"I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\5I\u03e1\nI\3J\3J\3J\3J\3J\7J\u03e8"+
		"\nJ\fJ\16J\u03eb\13J\3J\3J\3K\3K\3K\5K\u03f2\nK\3K\5K\u03f5\nK\3L\3L\5"+
		"L\u03f9\nL\3L\3L\5L\u03fd\nL\5L\u03ff\nL\3M\3M\3M\3M\3M\3M\5M\u0407\n"+
		"M\3N\3N\3N\3O\3O\3O\3O\3O\3P\3P\3P\3P\3P\3Q\3Q\3R\3R\3S\3S\3T\3T\3U\3"+
		"U\3U\3U\3U\3U\3U\3U\3U\3U\5U\u0428\nU\3V\3V\3W\3W\3W\3W\5W\u0430\nW\3"+
		"X\3X\3X\3X\5X\u0436\nX\3Y\3Y\3Z\3Z\3[\3[\3\\\3\\\3]\3]\3]\7]\u0443\n]"+
		"\f]\16]\u0446\13]\3^\3^\3^\7^\u044b\n^\f^\16^\u044e\13^\3_\3_\5_\u0452"+
		"\n_\3`\3`\3`\3`\3`\5`\u0459\n`\3`\3`\5`\u045d\n`\3`\3`\3`\3`\5`\u0463"+
		"\n`\3a\3a\3b\3b\5b\u0469\nb\3b\3b\3b\5b\u046e\nb\7b\u0470\nb\fb\16b\u0473"+
		"\13b\3c\3c\3c\3c\3c\5c\u047a\nc\5c\u047c\nc\3c\5c\u047f\nc\3c\3c\3d\3"+
		"d\3d\3d\3d\3d\3d\3d\3d\5d\u048c\nd\3e\3e\3e\7e\u0491\ne\fe\16e\u0494\13"+
		"e\3f\3f\5f\u0498\nf\3g\3g\3g\3g\3h\3h\3h\3h\3i\3i\3i\3i\3i\3j\3j\5j\u04a9"+
		"\nj\3k\3k\3k\3k\5k\u04af\nk\3k\3k\3k\3k\5k\u04b5\nk\7k\u04b7\nk\fk\16"+
		"k\u04ba\13k\3k\3k\3l\3l\3l\3l\5l\u04c2\nl\3l\5l\u04c5\nl\3m\3m\3m\3n\3"+
		"n\3n\3n\5n\u04ce\nn\3n\5n\u04d1\nn\3o\3o\3o\7o\u04d6\no\fo\16o\u04d9\13"+
		"o\3p\3p\3p\7p\u04de\np\fp\16p\u04e1\13p\3q\3q\3q\7q\u04e6\nq\fq\16q\u04e9"+
		"\13q\3q\3q\3q\5q\u04ee\nq\3r\3r\3r\3r\5r\u04f4\nr\3r\3r\3s\3s\3s\3s\3"+
		"s\5s\u04fd\ns\3s\3s\3s\3s\3s\3s\3s\3s\3s\3s\3s\5s\u050a\ns\3s\5s\u050d"+
		"\ns\3t\3t\3u\3u\3u\7u\u0514\nu\fu\16u\u0517\13u\3v\3v\5v\u051b\nv\3v\3"+
		"v\3v\5v\u0520\nv\5v\u0522\nv\3w\3w\3w\3w\3w\3w\3w\3w\3w\3w\3w\3w\3w\3"+
		"w\3w\3w\5w\u0534\nw\3x\3x\3x\3x\3x\3x\3x\3x\3x\3x\3x\3x\3x\3x\3x\5x\u0545"+
		"\nx\3x\3x\5x\u0549\nx\3y\3y\3y\3z\3z\3z\3z\3z\3z\5z\u0554\nz\3z\3z\3z"+
		"\3z\3z\5z\u055b\nz\3z\5z\u055e\nz\3z\5z\u0561\nz\3{\3{\3{\3{\3{\3{\3{"+
		"\3{\5{\u056b\n{\3|\3|\3|\7|\u0570\n|\f|\16|\u0573\13|\3}\3}\5}\u0577\n"+
		"}\3~\3~\7~\u057b\n~\f~\16~\u057e\13~\3\177\3\177\3\177\3\177\3\177\3\177"+
		"\5\177\u0586\n\177\3\u0080\3\u0080\3\u0080\3\u0080\5\u0080\u058c\n\u0080"+
		"\3\u0080\3\u0080\3\u0080\3\u0080\5\u0080\u0592\n\u0080\3\u0081\3\u0081"+
		"\3\u0081\5\u0081\u0597\n\u0081\3\u0081\3\u0081\3\u0081\3\u0081\3\u0081"+
		"\3\u0081\3\u0081\3\u0081\3\u0081\3\u0081\5\u0081\u05a3\n\u0081\3\u0081"+
		"\3\u0081\3\u0081\3\u0081\3\u0081\5\u0081\u05aa\n\u0081\3\u0082\3\u0082"+
		"\3\u0082\7\u0082\u05af\n\u0082\f\u0082\16\u0082\u05b2\13\u0082\3\u0083"+
		"\3\u0083\3\u0083\5\u0083\u05b7\n\u0083\3\u0083\3\u0083\3\u0083\3\u0083"+
		"\3\u0083\3\u0083\3\u0083\3\u0083\3\u0083\3\u0083\3\u0083\3\u0083\5\u0083"+
		"\u05c5\n\u0083\3\u0084\3\u0084\3\u0084\3\u0084\5\u0084\u05cb\n\u0084\3"+
		"\u0084\5\u0084\u05ce\n\u0084\3\u0085\3\u0085\3\u0085\3\u0085\3\u0086\3"+
		"\u0086\3\u0086\3\u0086\5\u0086\u05d8\n\u0086\3\u0086\5\u0086\u05db\n\u0086"+
		"\3\u0086\5\u0086\u05de\n\u0086\3\u0086\5\u0086\u05e1\n\u0086\3\u0086\5"+
		"\u0086\u05e4\n\u0086\3\u0087\3\u0087\3\u0087\3\u0087\5\u0087\u05ea\n\u0087"+
		"\3\u0088\3\u0088\3\u0088\3\u0088\3\u0089\3\u0089\3\u0089\3\u0089\5\u0089"+
		"\u05f4\n\u0089\3\u008a\3\u008a\3\u008a\3\u008a\5\u008a\u05fa\n\u008a\3"+
		"\u008b\3\u008b\5\u008b\u05fe\n\u008b\3\u008c\3\u008c\3\u008c\3\u008d\3"+
		"\u008d\3\u008d\5\u008d\u0606\n\u008d\3\u008d\5\u008d\u0609\n\u008d\3\u008d"+
		"\3\u008d\3\u008d\5\u008d\u060e\n\u008d\3\u008e\3\u008e\3\u008e\3\u008f"+
		"\3\u008f\3\u008f\3\u008f\3\u0090\3\u0090\5\u0090\u0619\n\u0090\3\u0091"+
		"\3\u0091\3\u0091\3\u0092\3\u0092\3\u0092\3\u0092\3\u0093\3\u0093\3\u0093"+
		"\3\u0093\3\u0094\3\u0094\3\u0094\3\u0094\3\u0094\3\u0094\3\u0095\3\u0095"+
		"\3\u0095\3\u0095\3\u0096\3\u0096\3\u0096\3\u0096\3\u0097\3\u0097\3\u0097"+
		"\3\u0097\3\u0097\3\u0097\3\u0098\3\u0098\3\u0098\3\u0098\5\u0098\u063e"+
		"\n\u0098\3\u0099\3\u0099\3\u0099\7\u0099\u0643\n\u0099\f\u0099\16\u0099"+
		"\u0646\13\u0099\3\u009a\3\u009a\5\u009a\u064a\n\u009a\3\u009b\3\u009b"+
		"\5\u009b\u064e\n\u009b\3\u009b\3\u009b\3\u009b\5\u009b\u0653\n\u009b\7"+
		"\u009b\u0655\n\u009b\f\u009b\16\u009b\u0658\13\u009b\3\u009c\3\u009c\3"+
		"\u009d\3\u009d\5\u009d\u065e\n\u009d\3\u009e\3\u009e\3\u009e\3\u009e\7"+
		"\u009e\u0664\n\u009e\f\u009e\16\u009e\u0667\13\u009e\3\u009e\3\u009e\3"+
		"\u009e\3\u009e\5\u009e\u066d\n\u009e\3\u009f\3\u009f\3\u009f\3\u009f\7"+
		"\u009f\u0673\n\u009f\f\u009f\16\u009f\u0676\13\u009f\3\u009f\3\u009f\3"+
		"\u009f\3\u009f\5\u009f\u067c\n\u009f\3\u00a0\3\u00a0\3\u00a0\3\u00a0\3"+
		"\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a1\5\u00a1\u0689\n"+
		"\u00a1\3\u00a2\3\u00a2\3\u00a2\3\u00a3\3\u00a3\3\u00a3\3\u00a4\3\u00a4"+
		"\3\u00a5\5\u00a5\u0694\n\u00a5\3\u00a5\3\u00a5\3\u00a6\3\u00a6\3\u00a7"+
		"\3\u00a7\3\u00a7\7\u00a7\u069d\n\u00a7\f\u00a7\16\u00a7\u06a0\13\u00a7"+
		"\3\u00a8\3\u00a8\3\u00a8\5\u00a8\u06a5\n\u00a8\3\u00a8\2\4\668\u00a9\2"+
		"\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJL"+
		"NPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e"+
		"\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6"+
		"\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be"+
		"\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6"+
		"\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee"+
		"\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\u0102\u0104\u0106"+
		"\u0108\u010a\u010c\u010e\u0110\u0112\u0114\u0116\u0118\u011a\u011c\u011e"+
		"\u0120\u0122\u0124\u0126\u0128\u012a\u012c\u012e\u0130\u0132\u0134\u0136"+
		"\u0138\u013a\u013c\u013e\u0140\u0142\u0144\u0146\u0148\u014a\u014c\u014e"+
		"\2\23\4\2\r\r\30\30\4\2##\64\64\3\2\u0086\u008b\3\2\u0092\u0093\4\2\u0082"+
		"\u0082\u0094\u0094\4\2\63\63__\4\2\25\25**\5\2\u0082\u0082\u0085\u0085"+
		"\u0092\u0092\3\2mn\5\2jjllpp\6\2ijmnpprr\4\2\30\30\32\32\4\2\67\67ZZ\6"+
		"\2\25\25**88NN\3\2\u0098\u009a\3\2\u009b\u009c\t\2\5\17\21#&?ADGagx\u009e"+
		"\u009e\u0701\2\u0150\3\2\2\2\4\u0164\3\2\2\2\6\u0167\3\2\2\2\b\u016b\3"+
		"\2\2\2\n\u0176\3\2\2\2\f\u0179\3\2\2\2\16\u017c\3\2\2\2\20\u017e\3\2\2"+
		"\2\22\u018f\3\2\2\2\24\u01a0\3\2\2\2\26\u01b4\3\2\2\2\30\u01bc\3\2\2\2"+
		"\32\u01c4\3\2\2\2\34\u01cb\3\2\2\2\36\u01d4\3\2\2\2 \u01d8\3\2\2\2\"\u01db"+
		"\3\2\2\2$\u01df\3\2\2\2&\u01ef\3\2\2\2(\u0213\3\2\2\2*\u021a\3\2\2\2,"+
		"\u021c\3\2\2\2.\u022a\3\2\2\2\60\u0230\3\2\2\2\62\u023a\3\2\2\2\64\u023d"+
		"\3\2\2\2\66\u0240\3\2\2\28\u024b\3\2\2\2:\u0257\3\2\2\2<\u025b\3\2\2\2"+
		">\u0266\3\2\2\2@\u0268\3\2\2\2B\u026b\3\2\2\2D\u0285\3\2\2\2F\u028e\3"+
		"\2\2\2H\u0296\3\2\2\2J\u0298\3\2\2\2L\u02a0\3\2\2\2N\u02ab\3\2\2\2P\u02ad"+
		"\3\2\2\2R\u02b5\3\2\2\2T\u02bf\3\2\2\2V\u02c1\3\2\2\2X\u02ca\3\2\2\2Z"+
		"\u02cc\3\2\2\2\\\u02d6\3\2\2\2^\u02e8\3\2\2\2`\u02ea\3\2\2\2b\u02f4\3"+
		"\2\2\2d\u02f6\3\2\2\2f\u02fa\3\2\2\2h\u0319\3\2\2\2j\u031b\3\2\2\2l\u0322"+
		"\3\2\2\2n\u0324\3\2\2\2p\u0332\3\2\2\2r\u0337\3\2\2\2t\u034c\3\2\2\2v"+
		"\u0353\3\2\2\2x\u0357\3\2\2\2z\u035f\3\2\2\2|\u0376\3\2\2\2~\u03b0\3\2"+
		"\2\2\u0080\u03b2\3\2\2\2\u0082\u03b6\3\2\2\2\u0084\u03bc\3\2\2\2\u0086"+
		"\u03bf\3\2\2\2\u0088\u03c7\3\2\2\2\u008a\u03c9\3\2\2\2\u008c\u03cb\3\2"+
		"\2\2\u008e\u03cd\3\2\2\2\u0090\u03e0\3\2\2\2\u0092\u03e2\3\2\2\2\u0094"+
		"\u03ee\3\2\2\2\u0096\u03fe\3\2\2\2\u0098\u0400\3\2\2\2\u009a\u0408\3\2"+
		"\2\2\u009c\u040b\3\2\2\2\u009e\u0410\3\2\2\2\u00a0\u0415\3\2\2\2\u00a2"+
		"\u0417\3\2\2\2\u00a4\u0419\3\2\2\2\u00a6\u041b\3\2\2\2\u00a8\u0427\3\2"+
		"\2\2\u00aa\u0429\3\2\2\2\u00ac\u042b\3\2\2\2\u00ae\u0431\3\2\2\2\u00b0"+
		"\u0437\3\2\2\2\u00b2\u0439\3\2\2\2\u00b4\u043b\3\2\2\2\u00b6\u043d\3\2"+
		"\2\2\u00b8\u043f\3\2\2\2\u00ba\u0447\3\2\2\2\u00bc\u0451\3\2\2\2\u00be"+
		"\u0453\3\2\2\2\u00c0\u0464\3\2\2\2\u00c2\u0468\3\2\2\2\u00c4\u0474\3\2"+
		"\2\2\u00c6\u048b\3\2\2\2\u00c8\u048d\3\2\2\2\u00ca\u0495\3\2\2\2\u00cc"+
		"\u0499\3\2\2\2\u00ce\u049d\3\2\2\2\u00d0\u04a1\3\2\2\2\u00d2\u04a8\3\2"+
		"\2\2\u00d4\u04aa\3\2\2\2\u00d6\u04bd\3\2\2\2\u00d8\u04c6\3\2\2\2\u00da"+
		"\u04c9\3\2\2\2\u00dc\u04d2\3\2\2\2\u00de\u04da\3\2\2\2\u00e0\u04ed\3\2"+
		"\2\2\u00e2\u04ef\3\2\2\2\u00e4\u04f7\3\2\2\2\u00e6\u050e\3\2\2\2\u00e8"+
		"\u0510\3\2\2\2\u00ea\u0521\3\2\2\2\u00ec\u0533\3\2\2\2\u00ee\u0544\3\2"+
		"\2\2\u00f0\u054a\3\2\2\2\u00f2\u054d\3\2\2\2\u00f4\u056a\3\2\2\2\u00f6"+
		"\u056c\3\2\2\2\u00f8\u0574\3\2\2\2\u00fa\u0578\3\2\2\2\u00fc\u0585\3\2"+
		"\2\2\u00fe\u0587\3\2\2\2\u0100\u0593\3\2\2\2\u0102\u05ab\3\2\2\2\u0104"+
		"\u05b3\3\2\2\2\u0106\u05c6\3\2\2\2\u0108\u05cf\3\2\2\2\u010a\u05d3\3\2"+
		"\2\2\u010c\u05e5\3\2\2\2\u010e\u05eb\3\2\2\2\u0110\u05ef\3\2\2\2\u0112"+
		"\u05f5\3\2\2\2\u0114\u05fd\3\2\2\2\u0116\u05ff\3\2\2\2\u0118\u060d\3\2"+
		"\2\2\u011a\u060f\3\2\2\2\u011c\u0612\3\2\2\2\u011e\u0616\3\2\2\2\u0120"+
		"\u061a\3\2\2\2\u0122\u061d\3\2\2\2\u0124\u0621\3\2\2\2\u0126\u0625\3\2"+
		"\2\2\u0128\u062b\3\2\2\2\u012a\u062f\3\2\2\2\u012c\u0633\3\2\2\2\u012e"+
		"\u063d\3\2\2\2\u0130\u063f\3\2\2\2\u0132\u0649\3\2\2\2\u0134\u064d\3\2"+
		"\2\2\u0136\u0659\3\2\2\2\u0138\u065d\3\2\2\2\u013a\u066c\3\2\2\2\u013c"+
		"\u067b\3\2\2\2\u013e\u067d\3\2\2\2\u0140\u0688\3\2\2\2\u0142\u068a\3\2"+
		"\2\2\u0144\u068d\3\2\2\2\u0146\u0690\3\2\2\2\u0148\u0693\3\2\2\2\u014a"+
		"\u0697\3\2\2\2\u014c\u0699\3\2\2\2\u014e\u06a4\3\2\2\2\u0150\u0151\5\4"+
		"\3\2\u0151\u0152\7\2\2\3\u0152\3\3\2\2\2\u0153\u0165\5\6\4\2\u0154\u0165"+
		"\5z>\2\u0155\u0165\5\u00be`\2\u0156\u0165\5\u00e4s\2\u0157\u0165\5\u0106"+
		"\u0084\2\u0158\u0165\5\u0108\u0085\2\u0159\u0165\5\u00fe\u0080\2\u015a"+
		"\u0165\5\u00f2z\2\u015b\u0165\5\u010e\u0088\2\u015c\u0165\5\u010c\u0087"+
		"\2\u015d\u0165\5\u00d0i\2\u015e\u0165\5\u010a\u0086\2\u015f\u0165\5\u00e2"+
		"r\2\u0160\u0165\5\u0110\u0089\2\u0161\u0165\5\u0112\u008a\2\u0162\u0165"+
		"\5\u0100\u0081\2\u0163\u0165\5\u0104\u0083\2\u0164\u0153\3\2\2\2\u0164"+
		"\u0154\3\2\2\2\u0164\u0155\3\2\2\2\u0164\u0156\3\2\2\2\u0164\u0157\3\2"+
		"\2\2\u0164\u0158\3\2\2\2\u0164\u0159\3\2\2\2\u0164\u015a\3\2\2\2\u0164"+
		"\u015b\3\2\2\2\u0164\u015c\3\2\2\2\u0164\u015d\3\2\2\2\u0164\u015e\3\2"+
		"\2\2\u0164\u015f\3\2\2\2\u0164\u0160\3\2\2\2\u0164\u0161\3\2\2\2\u0164"+
		"\u0162\3\2\2\2\u0164\u0163\3\2\2\2\u0165\5\3\2\2\2\u0166\u0168\5\b\5\2"+
		"\u0167\u0166\3\2\2\2\u0167\u0168\3\2\2\2\u0168\u0169\3\2\2\2\u0169\u016a"+
		"\5\20\t\2\u016a\7\3\2\2\2\u016b\u016c\7\26\2\2\u016c\u016d\5\n\6\2\u016d"+
		"\u0173\7y\2\2\u016e\u016f\5\n\6\2\u016f\u0170\7y\2\2\u0170\u0172\3\2\2"+
		"\2\u0171\u016e\3\2\2\2\u0172\u0175\3\2\2\2\u0173\u0171\3\2\2\2\u0173\u0174"+
		"\3\2\2\2\u0174\t\3\2\2\2\u0175\u0173\3\2\2\2\u0176\u0177\5\f\7\2\u0177"+
		"\u0178\5\u0090I\2\u0178\13\3\2\2\2\u0179\u017a\7\u0084\2\2\u017a\u017b"+
		"\5\u014e\u00a8\2\u017b\r\3\2\2\2\u017c\u017d\5\66\34\2\u017d\17\3\2\2"+
		"\2\u017e\u017f\5\"\22\2\u017f\u0181\5\22\n\2\u0180\u0182\5 \21\2\u0181"+
		"\u0180\3\2\2\2\u0181\u0182\3\2\2\2\u0182\u0184\3\2\2\2\u0183\u0185\5\60"+
		"\31\2\u0184\u0183\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u0187\3\2\2\2\u0186"+
		"\u0188\5,\27\2\u0187\u0186\3\2\2\2\u0187\u0188\3\2\2\2\u0188\u018a\3\2"+
		"\2\2\u0189\u018b\5\62\32\2\u018a\u0189\3\2\2\2\u018a\u018b\3\2\2\2\u018b"+
		"\u018d\3\2\2\2\u018c\u018e\5\64\33\2\u018d\u018c\3\2\2\2\u018d\u018e\3"+
		"\2\2\2\u018e\21\3\2\2\2\u018f\u0192\7&\2\2\u0190\u0193\5\32\16\2\u0191"+
		"\u0193\5\24\13\2\u0192\u0190\3\2\2\2\u0192\u0191\3\2\2\2\u0193\u019d\3"+
		"\2\2\2\u0194\u0195\7z\2\2\u0195\u0197\5\16\b\2\u0196\u0198\7\f\2\2\u0197"+
		"\u0196\3\2\2\2\u0197\u0198\3\2\2\2\u0198\u0199\3\2\2\2\u0199\u019a\5\f"+
		"\7\2\u019a\u019c\3\2\2\2\u019b\u0194\3\2\2\2\u019c\u019f\3\2\2\2\u019d"+
		"\u019b\3\2\2\2\u019d\u019e\3\2\2\2\u019e\23\3\2\2\2\u019f\u019d\3\2\2"+
		"\2\u01a0\u01a1\7:\2\2\u01a1\u01a2\7U\2\2\u01a2\u01a3\7|\2\2\u01a3\u01a9"+
		"\5\32\16\2\u01a4\u01a5\7\n\2\2\u01a5\u01a6\7|\2\2\u01a6\u01a7\5\26\f\2"+
		"\u01a7\u01a8\7}\2\2\u01a8\u01aa\3\2\2\2\u01a9\u01a4\3\2\2\2\u01a9\u01aa"+
		"\3\2\2\2\u01aa\u01b0\3\2\2\2\u01ab\u01ac\7\31\2\2\u01ac\u01ad\7|\2\2\u01ad"+
		"\u01ae\5\30\r\2\u01ae\u01af\7}\2\2\u01af\u01b1\3\2\2\2\u01b0\u01ab\3\2"+
		"\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2\u01b3\7}\2\2\u01b3"+
		"\25\3\2\2\2\u01b4\u01b9\5\32\16\2\u01b5\u01b6\7z\2\2\u01b6\u01b8\5\32"+
		"\16\2\u01b7\u01b5\3\2\2\2\u01b8\u01bb\3\2\2\2\u01b9\u01b7\3\2\2\2\u01b9"+
		"\u01ba\3\2\2\2\u01ba\27\3\2\2\2\u01bb\u01b9\3\2\2\2\u01bc\u01c1\5\32\16"+
		"\2\u01bd\u01be\7z\2\2\u01be\u01c0\5\32\16\2\u01bf\u01bd\3\2\2\2\u01c0"+
		"\u01c3\3\2\2\2\u01c1\u01bf\3\2\2\2\u01c1\u01c2\3\2\2\2\u01c2\31\3\2\2"+
		"\2\u01c3\u01c1\3\2\2\2\u01c4\u01c7\5\34\17\2\u01c5\u01c6\7?\2\2\u01c6"+
		"\u01c8\5\66\34\2\u01c7\u01c5\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8\33\3\2"+
		"\2\2\u01c9\u01cc\5\u00c0a\2\u01ca\u01cc\7\u009d\2\2\u01cb\u01c9\3\2\2"+
		"\2\u01cb\u01ca\3\2\2\2\u01cc\u01d1\3\2\2\2\u01cd\u01cf\7\f\2\2\u01ce\u01cd"+
		"\3\2\2\2\u01ce\u01cf\3\2\2\2\u01cf\u01d0\3\2\2\2\u01d0\u01d2\5\36\20\2"+
		"\u01d1\u01ce\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2\35\3\2\2\2\u01d3\u01d5"+
		"\7\u0084\2\2\u01d4\u01d3\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5\u01d6\3\2\2"+
		"\2\u01d6\u01d7\5\u014e\u00a8\2\u01d7\37\3\2\2\2\u01d8\u01d9\7a\2\2\u01d9"+
		"\u01da\5\16\b\2\u01da!\3\2\2\2\u01db\u01dc\7O\2\2\u01dc\u01dd\5$\23\2"+
		"\u01dd#\3\2\2\2\u01de\u01e0\5&\24\2\u01df\u01de\3\2\2\2\u01df\u01e0\3"+
		"\2\2\2\u01e0\u01ed\3\2\2\2\u01e1\u01ee\7\u0082\2\2\u01e2\u01e3\5\16\b"+
		"\2\u01e3\u01ea\5*\26\2\u01e4\u01e5\7z\2\2\u01e5\u01e6\5\16\b\2\u01e6\u01e7"+
		"\5*\26\2\u01e7\u01e9\3\2\2\2\u01e8\u01e4\3\2\2\2\u01e9\u01ec\3\2\2\2\u01ea"+
		"\u01e8\3\2\2\2\u01ea\u01eb\3\2\2\2\u01eb\u01ee\3\2\2\2\u01ec\u01ea\3\2"+
		"\2\2\u01ed\u01e1\3\2\2\2\u01ed\u01e2\3\2\2\2\u01ee%\3\2\2\2\u01ef\u01f3"+
		"\7\3\2\2\u01f0\u01f2\5(\25\2\u01f1\u01f0\3\2\2\2\u01f2\u01f5\3\2\2\2\u01f3"+
		"\u01f1\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4\u01f6\3\2\2\2\u01f5\u01f3\3\2"+
		"\2\2\u01f6\u01f7\7\4\2\2\u01f7\'\3\2\2\2\u01f8\u01f9\7E\2\2\u01f9\u01fa"+
		"\7|\2\2\u01fa\u01fe\5\u00c0a\2\u01fb\u01fd\5\u00e6t\2\u01fc\u01fb\3\2"+
		"\2\2\u01fd\u0200\3\2\2\2\u01fe\u01fc\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff"+
		"\u0201\3\2\2\2\u0200\u01fe\3\2\2\2\u0201\u0202\7}\2\2\u0202\u0214\3\2"+
		"\2\2\u0203\u0204\7$\2\2\u0204\u0205\7|\2\2\u0205\u0206\5\u00c0a\2\u0206"+
		"\u0207\5\u00e6t\2\u0207\u0208\7}\2\2\u0208\u0214\3\2\2\2\u0209\u020a\7"+
		"F\2\2\u020a\u020b\7|\2\2\u020b\u020c\5\u00c0a\2\u020c\u020d\7}\2\2\u020d"+
		"\u0214\3\2\2\2\u020e\u020f\7%\2\2\u020f\u0210\7|\2\2\u0210\u0211\5\u00c0"+
		"a\2\u0211\u0212\7}\2\2\u0212\u0214\3\2\2\2\u0213\u01f8\3\2\2\2\u0213\u0203"+
		"\3\2\2\2\u0213\u0209\3\2\2\2\u0213\u020e\3\2\2\2\u0214\u0216\3\2\2\2\u0215"+
		"\u0217\7\u009c\2\2\u0216\u0215\3\2\2\2\u0216\u0217\3\2\2\2\u0217)\3\2"+
		"\2\2\u0218\u0219\7\f\2\2\u0219\u021b\5\u014e\u00a8\2\u021a\u0218\3\2\2"+
		"\2\u021a\u021b\3\2\2\2\u021b+\3\2\2\2\u021c\u021d\7B\2\2\u021d\u021e\7"+
		"\16\2\2\u021e\u021f\5\16\b\2\u021f\u0226\5.\30\2\u0220\u0221\7z\2\2\u0221"+
		"\u0222\5\16\b\2\u0222\u0223\5.\30\2\u0223\u0225\3\2\2\2\u0224\u0220\3"+
		"\2\2\2\u0225\u0228\3\2\2\2\u0226\u0224\3\2\2\2\u0226\u0227\3\2\2\2\u0227"+
		"-\3\2\2\2\u0228\u0226\3\2\2\2\u0229\u022b\t\2\2\2\u022a\u0229\3\2\2\2"+
		"\u022a\u022b\3\2\2\2\u022b\u022e\3\2\2\2\u022c\u022d\7<\2\2\u022d\u022f"+
		"\t\3\2\2\u022e\u022c\3\2\2\2\u022e\u022f\3\2\2\2\u022f/\3\2\2\2\u0230"+
		"\u0231\7)\2\2\u0231\u0232\7\16\2\2\u0232\u0237\5\16\b\2\u0233\u0234\7"+
		"z\2\2\u0234\u0236\5\16\b\2\u0235\u0233\3\2\2\2\u0236\u0239\3\2\2\2\u0237"+
		"\u0235\3\2\2\2\u0237\u0238\3\2\2\2\u0238\61\3\2\2\2\u0239\u0237\3\2\2"+
		"\2\u023a\u023b\7\66\2\2\u023b\u023c\5J&\2\u023c\63\3\2\2\2\u023d\u023e"+
		"\7=\2\2\u023e\u023f\5J&\2\u023f\65\3\2\2\2\u0240\u0241\b\34\1\2\u0241"+
		"\u0242\58\35\2\u0242\u0248\3\2\2\2\u0243\u0244\f\3\2\2\u0244\u0245\7A"+
		"\2\2\u0245\u0247\58\35\2\u0246\u0243\3\2\2\2\u0247\u024a\3\2\2\2\u0248"+
		"\u0246\3\2\2\2\u0248\u0249\3\2\2\2\u0249\67\3\2\2\2\u024a\u0248\3\2\2"+
		"\2\u024b\u024c\b\35\1\2\u024c\u024d\5:\36\2\u024d\u0253\3\2\2\2\u024e"+
		"\u024f\f\3\2\2\u024f\u0250\7\13\2\2\u0250\u0252\5:\36\2\u0251\u024e\3"+
		"\2\2\2\u0252\u0255\3\2\2\2\u0253\u0251\3\2\2\2\u0253\u0254\3\2\2\2\u0254"+
		"9\3\2\2\2\u0255\u0253\3\2\2\2\u0256\u0258\7;\2\2\u0257\u0256\3\2\2\2\u0257"+
		"\u0258\3\2\2\2\u0258\u0259\3\2\2\2\u0259\u025a\5<\37\2\u025a;\3\2\2\2"+
		"\u025b\u0261\5> \2\u025c\u025e\7/\2\2\u025d\u025f\7;\2\2\u025e\u025d\3"+
		"\2\2\2\u025e\u025f\3\2\2\2\u025f\u0260\3\2\2\2\u0260\u0262\7\u0095\2\2"+
		"\u0261\u025c\3\2\2\2\u0261\u0262\3\2\2\2\u0262=\3\2\2\2\u0263\u0267\5"+
		"D#\2\u0264\u0267\5@!\2\u0265\u0267\5B\"\2\u0266\u0263\3\2\2\2\u0266\u0264"+
		"\3\2\2\2\u0266\u0265\3\2\2\2\u0267?\3\2\2\2\u0268\u0269\7!\2\2\u0269\u026a"+
		"\5J&\2\u026aA\3\2\2\2\u026b\u026c\5J&\2\u026c\u026e\7/\2\2\u026d\u026f"+
		"\7;\2\2\u026e\u026d\3\2\2\2\u026e\u026f\3\2\2\2\u026f\u0270\3\2\2\2\u0270"+
		"\u0272\7>\2\2\u0271\u0273\7Y\2\2\u0272\u0271\3\2\2\2\u0272\u0273\3\2\2"+
		"\2\u0273\u0274\3\2\2\2\u0274\u0276\7|\2\2\u0275\u0277\7@\2\2\u0276\u0275"+
		"\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u0278\3\2\2\2\u0278\u0280\5\u008eH"+
		"\2\u0279\u027b\7z\2\2\u027a\u027c\7@\2\2\u027b\u027a\3\2\2\2\u027b\u027c"+
		"\3\2\2\2\u027c\u027d\3\2\2\2\u027d\u027f\5\u008eH\2\u027e\u0279\3\2\2"+
		"\2\u027f\u0282\3\2\2\2\u0280\u027e\3\2\2\2\u0280\u0281\3\2\2\2\u0281\u0283"+
		"\3\2\2\2\u0282\u0280\3\2\2\2\u0283\u0284\7}\2\2\u0284C\3\2\2\2\u0285\u028c"+
		"\5J&\2\u0286\u0289\5F$\2\u0287\u0289\5H%\2\u0288\u0286\3\2\2\2\u0288\u0287"+
		"\3\2\2\2\u0289\u028a\3\2\2\2\u028a\u028b\5J&\2\u028b\u028d\3\2\2\2\u028c"+
		"\u0288\3\2\2\2\u028c\u028d\3\2\2\2\u028dE\3\2\2\2\u028e\u028f\t\4\2\2"+
		"\u028fG\3\2\2\2\u0290\u0297\7\u0090\2\2\u0291\u0297\7\u0091\2\2\u0292"+
		"\u0297\7\u008e\2\2\u0293\u0297\7\u008f\2\2\u0294\u0297\7\u008c\2\2\u0295"+
		"\u0297\7\u008d\2\2\u0296\u0290\3\2\2\2\u0296\u0291\3\2\2\2\u0296\u0292"+
		"\3\2\2\2\u0296\u0293\3\2\2\2\u0296\u0294\3\2\2\2\u0296\u0295\3\2\2\2\u0297"+
		"I\3\2\2\2\u0298\u029d\5L\'\2\u0299\u029a\t\5\2\2\u029a\u029c\5L\'\2\u029b"+
		"\u0299\3\2\2\2\u029c\u029f\3\2\2\2\u029d\u029b\3\2\2\2\u029d\u029e\3\2"+
		"\2\2\u029eK\3\2\2\2\u029f\u029d\3\2\2\2\u02a0\u02a5\5N(\2\u02a1\u02a2"+
		"\t\6\2\2\u02a2\u02a4\5N(\2\u02a3\u02a1\3\2\2\2\u02a4\u02a7\3\2\2\2\u02a5"+
		"\u02a3\3\2\2\2\u02a5\u02a6\3\2\2\2\u02a6M\3\2\2\2\u02a7\u02a5\3\2\2\2"+
		"\u02a8\u02ac\5P)\2\u02a9\u02aa\t\5\2\2\u02aa\u02ac\5N(\2\u02ab\u02a8\3"+
		"\2\2\2\u02ab\u02a9\3\2\2\2\u02acO\3\2\2\2\u02ad\u02b2\5^\60\2\u02ae\u02b1"+
		"\5R*\2\u02af\u02b1\5X-\2\u02b0\u02ae\3\2\2\2\u02b0\u02af\3\2\2\2\u02b1"+
		"\u02b4\3\2\2\2\u02b2\u02b0\3\2\2\2\u02b2\u02b3\3\2\2\2\u02b3Q\3\2\2\2"+
		"\u02b4\u02b2\3\2\2\2\u02b5\u02b8\7\u0083\2\2\u02b6\u02b9\5V,\2\u02b7\u02b9"+
		"\5T+\2\u02b8\u02b6\3\2\2\2\u02b8\u02b7\3\2\2\2\u02b9S\3\2\2\2\u02ba\u02c0"+
		"\5\u014e\u00a8\2\u02bb\u02c0\5\u014a\u00a6\2\u02bc\u02c0\5d\63\2\u02bd"+
		"\u02c0\5v<\2\u02be\u02c0\5n8\2\u02bf\u02ba\3\2\2\2\u02bf\u02bb\3\2\2\2"+
		"\u02bf\u02bc\3\2\2\2\u02bf\u02bd\3\2\2\2\u02bf\u02be\3\2\2\2\u02c0U\3"+
		"\2\2\2\u02c1\u02c2\t\7\2\2\u02c2\u02c4\7|\2\2\u02c3\u02c5\5\16\b\2\u02c4"+
		"\u02c3\3\2\2\2\u02c4\u02c5\3\2\2\2\u02c5\u02c6\3\2\2\2\u02c6\u02c7\7}"+
		"\2\2\u02c7W\3\2\2\2\u02c8\u02cb\5\\/\2\u02c9\u02cb\5Z.\2\u02ca\u02c8\3"+
		"\2\2\2\u02ca\u02c9\3\2\2\2\u02cbY\3\2\2\2\u02cc\u02ce\7~\2\2\u02cd\u02cf"+
		"\5\16\b\2\u02ce\u02cd\3\2\2\2\u02ce\u02cf\3\2\2\2\u02cf\u02d0\3\2\2\2"+
		"\u02d0\u02d2\7{\2\2\u02d1\u02d3\5\16\b\2\u02d2\u02d1\3\2\2\2\u02d2\u02d3"+
		"\3\2\2\2\u02d3\u02d4\3\2\2\2\u02d4\u02d5\7\177\2\2\u02d5[\3\2\2\2\u02d6"+
		"\u02d8\7~\2\2\u02d7\u02d9\5\16\b\2\u02d8\u02d7\3\2\2\2\u02d8\u02d9\3\2"+
		"\2\2\u02d9\u02da\3\2\2\2\u02da\u02db\7\177\2\2\u02db]\3\2\2\2\u02dc\u02e9"+
		"\5b\62\2\u02dd\u02e9\5`\61\2\u02de\u02e9\5d\63\2\u02df\u02e9\5f\64\2\u02e0"+
		"\u02e9\5h\65\2\u02e1\u02e9\5j\66\2\u02e2\u02e9\5n8\2\u02e3\u02e9\5p9\2"+
		"\u02e4\u02e9\5r:\2\u02e5\u02e9\5t;\2\u02e6\u02e9\5v<\2\u02e7\u02e9\5x"+
		"=\2\u02e8\u02dc\3\2\2\2\u02e8\u02dd\3\2\2\2\u02e8\u02de\3\2\2\2\u02e8"+
		"\u02df\3\2\2\2\u02e8\u02e0\3\2\2\2\u02e8\u02e1\3\2\2\2\u02e8\u02e2\3\2"+
		"\2\2\u02e8\u02e3\3\2\2\2\u02e8\u02e4\3\2\2\2\u02e8\u02e5\3\2\2\2\u02e8"+
		"\u02e6\3\2\2\2\u02e8\u02e7\3\2\2\2\u02e9_\3\2\2\2\u02ea\u02ed\5\u014e"+
		"\u00a8\2\u02eb\u02ec\7\u0083\2\2\u02ec\u02ee\5\u014e\u00a8\2\u02ed\u02eb"+
		"\3\2\2\2\u02ed\u02ee\3\2\2\2\u02eea\3\2\2\2\u02ef\u02f5\5\u0148\u00a5"+
		"\2\u02f0\u02f5\5\u014a\u00a6\2\u02f1\u02f5\7\u0097\2\2\u02f2\u02f5\7\u0096"+
		"\2\2\u02f3\u02f5\7\u0095\2\2\u02f4\u02ef\3\2\2\2\u02f4\u02f0\3\2\2\2\u02f4"+
		"\u02f1\3\2\2\2\u02f4\u02f2\3\2\2\2\u02f4\u02f3\3\2\2\2\u02f5c\3\2\2\2"+
		"\u02f6\u02f8\7\u0084\2\2\u02f7\u02f9\5\u014e\u00a8\2\u02f8\u02f7\3\2\2"+
		"\2\u02f8\u02f9\3\2\2\2\u02f9e\3\2\2\2\u02fa\u02fc\7~\2\2\u02fb\u02fd\5"+
		"\16\b\2\u02fc\u02fb\3\2\2\2\u02fc\u02fd\3\2\2\2\u02fd\u0302\3\2\2\2\u02fe"+
		"\u02ff\7z\2\2\u02ff\u0301\5\16\b\2\u0300\u02fe\3\2\2\2\u0301\u0304\3\2"+
		"\2\2\u0302\u0300\3\2\2\2\u0302\u0303\3\2\2\2\u0303\u0305\3\2\2\2\u0304"+
		"\u0302\3\2\2\2\u0305\u0306\7\177\2\2\u0306g\3\2\2\2\u0307\u0308\7\u0080"+
		"\2\2\u0308\u0309\5\16\b\2\u0309\u030a\7{\2\2\u030a\u0312\5\16\b\2\u030b"+
		"\u030c\7z\2\2\u030c\u030d\5\16\b\2\u030d\u030e\7{\2\2\u030e\u030f\5\16"+
		"\b\2\u030f\u0311\3\2\2\2\u0310\u030b\3\2\2\2\u0311\u0314\3\2\2\2\u0312"+
		"\u0310\3\2\2\2\u0312\u0313\3\2\2\2\u0313\u0315\3\2\2\2\u0314\u0312\3\2"+
		"\2\2\u0315\u0316\7\u0081\2\2\u0316\u031a\3\2\2\2\u0317\u0318\7\u0080\2"+
		"\2\u0318\u031a\7\u0081\2\2\u0319\u0307\3\2\2\2\u0319\u0317\3\2\2\2\u031a"+
		"i\3\2\2\2\u031b\u031c\7P\2\2\u031c\u031d\7|\2\2\u031d\u031e\5l\67\2\u031e"+
		"\u031f\7z\2\2\u031f\u0320\5\16\b\2\u0320\u0321\7}\2\2\u0321k\3\2\2\2\u0322"+
		"\u0323\5\16\b\2\u0323m\3\2\2\2\u0324\u0325\5\u014e\u00a8\2\u0325\u032e"+
		"\7|\2\2\u0326\u032b\5\16\b\2\u0327\u0328\7z\2\2\u0328\u032a\5\16\b\2\u0329"+
		"\u0327\3\2\2\2\u032a\u032d\3\2\2\2\u032b\u0329\3\2\2\2\u032b\u032c\3\2"+
		"\2\2\u032c\u032f\3\2\2\2\u032d\u032b\3\2\2\2\u032e\u0326\3\2\2\2\u032e"+
		"\u032f\3\2\2\2\u032f\u0330\3\2\2\2\u0330\u0331\7}\2\2\u0331o\3\2\2\2\u0332"+
		"\u0333\7\23\2\2\u0333\u0334\7|\2\2\u0334\u0335\7\u0082\2\2\u0335\u0336"+
		"\7}\2\2\u0336q\3\2\2\2\u0337\u0338\7\17\2\2\u0338\u0339\7`\2\2\u0339\u033a"+
		"\5\16\b\2\u033a\u033b\7V\2\2\u033b\u0343\5\16\b\2\u033c\u033d\7`\2\2\u033d"+
		"\u033e\5\16\b\2\u033e\u033f\7V\2\2\u033f\u0340\5\16\b\2\u0340\u0342\3"+
		"\2\2\2\u0341\u033c\3\2\2\2\u0342\u0345\3\2\2\2\u0343\u0341\3\2\2\2\u0343"+
		"\u0344\3\2\2\2\u0344\u0348\3\2\2\2\u0345\u0343\3\2\2\2\u0346\u0347\7\35"+
		"\2\2\u0347\u0349\5\16\b\2\u0348\u0346\3\2\2\2\u0348\u0349\3\2\2\2\u0349"+
		"\u034a\3\2\2\2\u034a\u034b\7\36\2\2\u034bs\3\2\2\2\u034c\u034d\7\21\2"+
		"\2\u034d\u034e\7|\2\2\u034e\u034f\5\16\b\2\u034f\u0350\7\f\2\2\u0350\u0351"+
		"\5\u008eH\2\u0351\u0352\7}\2\2\u0352u\3\2\2\2\u0353\u0354\7|\2\2\u0354"+
		"\u0355\5\16\b\2\u0355\u0356\7}\2\2\u0356w\3\2\2\2\u0357\u0358\7\"\2\2"+
		"\u0358\u0359\7|\2\2\u0359\u035a\5\u014e\u00a8\2\u035a\u035b\7&\2\2\u035b"+
		"\u035c\5\16\b\2\u035c\u035d\7}\2\2\u035dy\3\2\2\2\u035e\u0360\5\b\5\2"+
		"\u035f\u035e\3\2\2\2\u035f\u0360\3\2\2\2\u0360\u0361\3\2\2\2\u0361\u0362"+
		"\7[\2\2\u0362\u0364\5\u00c0a\2\u0363\u0365\7\f\2\2\u0364\u0363\3\2\2\2"+
		"\u0364\u0365\3\2\2\2\u0365\u0367\3\2\2\2\u0366\u0368\5\36\20\2\u0367\u0366"+
		"\3\2\2\2\u0367\u0368\3\2\2\2\u0368\u0369\3\2\2\2\u0369\u036e\5~@\2\u036a"+
		"\u036b\7z\2\2\u036b\u036d\5~@\2\u036c\u036a\3\2\2\2\u036d\u0370\3\2\2"+
		"\2\u036e\u036c\3\2\2\2\u036e\u036f\3\2\2\2\u036f\u0371\3\2\2\2\u0370\u036e"+
		"\3\2\2\2\u0371\u0372\7a\2\2\u0372\u0374\5\16\b\2\u0373\u0375\5|?\2\u0374"+
		"\u0373\3\2\2\2\u0374\u0375\3\2\2\2\u0375{\3\2\2\2\u0376\u0377\7J\2\2\u0377"+
		"\u0378\5$\23\2\u0378}\3\2\2\2\u0379\u037a\7Q\2\2\u037a\u0382\5\u0080A"+
		"\2\u037b\u037e\7z\2\2\u037c\u037f\5~@\2\u037d\u037f\5\u0080A\2\u037e\u037c"+
		"\3\2\2\2\u037e\u037d\3\2\2\2\u037f\u0381\3\2\2\2\u0380\u037b\3\2\2\2\u0381"+
		"\u0384\3\2\2\2\u0382\u0380\3\2\2\2\u0382\u0383\3\2\2\2\u0383\u03b1\3\2"+
		"\2\2\u0384\u0382\3\2\2\2\u0385\u0386\7\6\2\2\u0386\u038e\5\u0082B\2\u0387"+
		"\u038a\7z\2\2\u0388\u038b\5~@\2\u0389\u038b\5\u0082B\2\u038a\u0388\3\2"+
		"\2\2\u038a\u0389\3\2\2\2\u038b\u038d\3\2\2\2\u038c\u0387\3\2\2\2\u038d"+
		"\u0390\3\2\2\2\u038e\u038c\3\2\2\2\u038e\u038f\3\2\2\2\u038f\u03b1\3\2"+
		"\2\2\u0390\u038e\3\2\2\2\u0391\u0392\7H\2\2\u0392\u039a\5\u0084C\2\u0393"+
		"\u0396\7z\2\2\u0394\u0397\5~@\2\u0395\u0397\5\u0084C\2\u0396\u0394\3\2"+
		"\2\2\u0396\u0395\3\2\2\2\u0397\u0399\3\2\2\2\u0398\u0393\3\2\2\2\u0399"+
		"\u039c\3\2\2\2\u039a\u0398\3\2\2\2\u039a\u039b\3\2\2\2\u039b\u03b1\3\2"+
		"\2\2\u039c\u039a\3\2\2\2\u039d\u039e\7I\2\2\u039e\u03a3\5\u0086D\2\u039f"+
		"\u03a0\7z\2\2\u03a0\u03a2\5\u0086D\2\u03a1\u039f\3\2\2\2\u03a2\u03a5\3"+
		"\2\2\2\u03a3\u03a1\3\2\2\2\u03a3\u03a4\3\2\2\2\u03a4\u03b1\3\2\2\2\u03a5"+
		"\u03a3\3\2\2\2\u03a6\u03a7\7Q\2\2\u03a7\u03a8\7X\2\2\u03a8\u03ad\5\u0088"+
		"E\2\u03a9\u03aa\7z\2\2\u03aa\u03ac\5~@\2\u03ab\u03a9\3\2\2\2\u03ac\u03af"+
		"\3\2\2\2\u03ad\u03ab\3\2\2\2\u03ad\u03ae\3\2\2\2\u03ae\u03b1\3\2\2\2\u03af"+
		"\u03ad\3\2\2\2\u03b0\u0379\3\2\2\2\u03b0\u0385\3\2\2\2\u03b0\u0391\3\2"+
		"\2\2\u03b0\u039d\3\2\2\2\u03b0\u03a6\3\2\2\2\u03b1\177\3\2\2\2\u03b2\u03b3"+
		"\5\u008aF\2\u03b3\u03b4\7\u008a\2\2\u03b4\u03b5\5\16\b\2\u03b5\u0081\3"+
		"\2\2\2\u03b6\u03b8\5\u008aF\2\u03b7\u03b9\5\u008cG\2\u03b8\u03b7\3\2\2"+
		"\2\u03b8\u03b9\3\2\2\2\u03b9\u03ba\3\2\2\2\u03ba\u03bb\5\16\b\2\u03bb"+
		"\u0083\3\2\2\2\u03bc\u03bd\5\u008aF\2\u03bd\u03be\5\16\b\2\u03be\u0085"+
		"\3\2\2\2\u03bf\u03c0\5\u008aF\2\u03c0\u0087\3\2\2\2\u03c1\u03c2\5J&\2"+
		"\u03c2\u03c3\t\b\2\2\u03c3\u03c8\3\2\2\2\u03c4\u03c5\7^\2\2\u03c5\u03c6"+
		"\7T\2\2\u03c6\u03c8\7\27\2\2\u03c7\u03c1\3\2\2\2\u03c7\u03c4\3\2\2\2\u03c8"+
		"\u0089\3\2\2\2\u03c9\u03ca\5P)\2\u03ca\u008b\3\2\2\2\u03cb\u03cc\5J&\2"+
		"\u03cc\u008d\3\2\2\2\u03cd\u03cf\5\u0090I\2\u03ce\u03d0\t\t\2\2\u03cf"+
		"\u03ce\3\2\2\2\u03cf\u03d0\3\2\2\2\u03d0\u008f\3\2\2\2\u03d1\u03e1\5\u00ac"+
		"W\2\u03d2\u03e1\5\u009eP\2\u03d3\u03e1\5\u00aaV\2\u03d4\u03e1\5\u00a8"+
		"U\2\u03d5\u03e1\5\u00a4S\2\u03d6\u03e1\5\u00a0Q\2\u03d7\u03e1\5\u00a2"+
		"R\2\u03d8\u03e1\5\u009cO\2\u03d9\u03e1\5\u0092J\2\u03da\u03e1\5\u00a6"+
		"T\2\u03db\u03e1\5\u00aeX\2\u03dc\u03e1\5\u00b0Y\2\u03dd\u03e1\5\u00b2"+
		"Z\2\u03de\u03e1\5\u00b4[\2\u03df\u03e1\5\u00b6\\\2\u03e0\u03d1\3\2\2\2"+
		"\u03e0\u03d2\3\2\2\2\u03e0\u03d3\3\2\2\2\u03e0\u03d4\3\2\2\2\u03e0\u03d5"+
		"\3\2\2\2\u03e0\u03d6\3\2\2\2\u03e0\u03d7\3\2\2\2\u03e0\u03d8\3\2\2\2\u03e0"+
		"\u03d9\3\2\2\2\u03e0\u03da\3\2\2\2\u03e0\u03db\3\2\2\2\u03e0\u03dc\3\2"+
		"\2\2\u03e0\u03dd\3\2\2\2\u03e0\u03de\3\2\2\2\u03e0\u03df\3\2\2\2\u03e1"+
		"\u0091\3\2\2\2\u03e2\u03e3\7q\2\2\u03e3\u03e4\7|\2\2\u03e4\u03e9\5\u0094"+
		"K\2\u03e5\u03e6\7z\2\2\u03e6\u03e8\5\u0094K\2\u03e7\u03e5\3\2\2\2\u03e8"+
		"\u03eb\3\2\2\2\u03e9\u03e7\3\2\2\2\u03e9\u03ea\3\2\2\2\u03ea\u03ec\3\2"+
		"\2\2\u03eb\u03e9\3\2\2\2\u03ec\u03ed\7}\2\2\u03ed\u0093\3\2\2\2\u03ee"+
		"\u03ef\5\u014e\u00a8\2\u03ef\u03f1\5\u0090I\2\u03f0\u03f2\5\u0096L\2\u03f1"+
		"\u03f0\3\2\2\2\u03f1\u03f2\3\2\2\2\u03f2\u03f4\3\2\2\2\u03f3\u03f5\5\u0142"+
		"\u00a2\2\u03f4\u03f3\3\2\2\2\u03f4\u03f5\3\2\2\2\u03f5\u0095\3\2\2\2\u03f6"+
		"\u03f8\5\u0098M\2\u03f7\u03f9\5\u009aN\2\u03f8\u03f7\3\2\2\2\u03f8\u03f9"+
		"\3\2\2\2\u03f9\u03ff\3\2\2\2\u03fa\u03fc\5\u009aN\2\u03fb\u03fd\5\u0098"+
		"M\2\u03fc\u03fb\3\2\2\2\u03fc\u03fd\3\2\2\2\u03fd\u03ff\3\2\2\2\u03fe"+
		"\u03f6\3\2\2\2\u03fe\u03fa\3\2\2\2\u03ff\u0097\3\2\2\2\u0400\u0406\7\27"+
		"\2\2\u0401\u0407\5\u0148\u00a5\2\u0402\u0407\5\u014a\u00a6\2\u0403\u0407"+
		"\7\u0097\2\2\u0404\u0407\7\u0096\2\2\u0405\u0407\5\u014e\u00a8\2\u0406"+
		"\u0401\3\2\2\2\u0406\u0402\3\2\2\2\u0406\u0403\3\2\2\2\u0406\u0404\3\2"+
		"\2\2\u0406\u0405\3\2\2\2\u0407\u0099\3\2\2\2\u0408\u0409\7;\2\2\u0409"+
		"\u040a\7\u0095\2\2\u040a\u009b\3\2\2\2\u040b\u040c\7o\2\2\u040c\u040d"+
		"\7|\2\2\u040d\u040e\5\u0090I\2\u040e\u040f\7}\2\2\u040f\u009d\3\2\2\2"+
		"\u0410\u0411\7g\2\2\u0411\u0412\7|\2\2\u0412\u0413\5\u0090I\2\u0413\u0414"+
		"\7}\2\2\u0414\u009f\3\2\2\2\u0415\u0416\t\n\2\2\u0416\u00a1\3\2\2\2\u0417"+
		"\u0418\7\60\2\2\u0418\u00a3\3\2\2\2\u0419\u041a\t\13\2\2\u041a\u00a5\3"+
		"\2\2\2\u041b\u041c\7r\2\2\u041c\u00a7\3\2\2\2\u041d\u041e\7k\2\2\u041e"+
		"\u041f\7|\2\2\u041f\u0420\5\u014c\u00a7\2\u0420\u0421\7}\2\2\u0421\u0428"+
		"\3\2\2\2\u0422\u0423\7k\2\2\u0423\u0424\7|\2\2\u0424\u0425\5\u014c\u00a7"+
		"\2\u0425\u0426\bU\1\2\u0426\u0428\3\2\2\2\u0427\u041d\3\2\2\2\u0427\u0422"+
		"\3\2\2\2\u0428\u00a9\3\2\2\2\u0429\u042a\7i\2\2\u042a\u00ab\3\2\2\2\u042b"+
		"\u042f\7h\2\2\u042c\u042d\7|\2\2\u042d\u042e\7\u0098\2\2\u042e\u0430\7"+
		"}\2\2\u042f\u042c\3\2\2\2\u042f\u0430\3\2\2\2\u0430\u00ad\3\2\2\2\u0431"+
		"\u0435\7s\2\2\u0432\u0433\7|\2\2\u0433\u0434\7\u0098\2\2\u0434\u0436\7"+
		"}\2\2\u0435\u0432\3\2\2\2\u0435\u0436\3\2\2\2\u0436\u00af\3\2\2\2\u0437"+
		"\u0438\7t\2\2\u0438\u00b1\3\2\2\2\u0439\u043a\7u\2\2\u043a\u00b3\3\2\2"+
		"\2\u043b\u043c\7v\2\2\u043c\u00b5\3\2\2\2\u043d\u043e\7w\2\2\u043e\u00b7"+
		"\3\2\2\2\u043f\u0444\5\u014e\u00a8\2\u0440\u0441\7\u0083\2\2\u0441\u0443"+
		"\5\u014e\u00a8\2\u0442\u0440\3\2\2\2\u0443\u0446\3\2\2\2\u0444\u0442\3"+
		"\2\2\2\u0444\u0445\3\2\2\2\u0445\u00b9\3\2\2\2\u0446\u0444\3\2\2\2\u0447"+
		"\u044c\5\u00bc_\2\u0448\u0449\7\u0083\2\2\u0449\u044b\5\u00bc_\2\u044a"+
		"\u0448\3\2\2\2\u044b\u044e\3\2\2\2\u044c\u044a\3\2\2\2\u044c\u044d\3\2"+
		"\2\2\u044d\u00bb\3\2\2\2\u044e\u044c\3\2\2\2\u044f\u0452\5\u014e\u00a8"+
		"\2\u0450\u0452\7\u009b\2\2\u0451\u044f\3\2\2\2\u0451\u0450\3\2\2\2\u0452"+
		"\u00bd\3\2\2\2\u0453\u0454\7\24\2\2\u0454\u0458\7T\2\2\u0455\u0456\7,"+
		"\2\2\u0456\u0457\7;\2\2\u0457\u0459\7!\2\2\u0458\u0455\3\2\2\2\u0458\u0459"+
		"\3\2\2\2\u0459\u045a\3\2\2\2\u045a\u045c\5\u00c0a\2\u045b\u045d\5\u0142"+
		"\u00a2\2\u045c\u045b\3\2\2\2\u045c\u045d\3\2\2\2\u045d\u045e\3\2\2\2\u045e"+
		"\u045f\7|\2\2\u045f\u0460\5\u00c2b\2\u0460\u0462\7}\2\2\u0461\u0463\5"+
		"\u00ceh\2\u0462\u0461\3\2\2\2\u0462\u0463\3\2\2\2\u0463\u00bf\3\2\2\2"+
		"\u0464\u0465\5\u00b8]\2\u0465\u00c1\3\2\2\2\u0466\u0469\5\u0094K\2\u0467"+
		"\u0469\5\u00c4c\2\u0468\u0466\3\2\2\2\u0468\u0467\3\2\2\2\u0469\u0471"+
		"\3\2\2\2\u046a\u046d\7z\2\2\u046b\u046e\5\u0094K\2\u046c\u046e\5\u00c4"+
		"c\2\u046d\u046b\3\2\2\2\u046d\u046c\3\2\2\2\u046e\u0470\3\2\2\2\u046f"+
		"\u046a\3\2\2\2\u0470\u0473\3\2\2\2\u0471\u046f\3\2\2\2\u0471\u0472\3\2"+
		"\2\2\u0472\u00c3\3\2\2\2\u0473\u0471\3\2\2\2\u0474\u0475\7G\2\2\u0475"+
		"\u0476\7\61\2\2\u0476\u047b\7|\2\2\u0477\u0479\5\u00c6d\2\u0478\u047a"+
		"\7z\2\2\u0479\u0478\3\2\2\2\u0479\u047a\3\2\2\2\u047a\u047c\3\2\2\2\u047b"+
		"\u0477\3\2\2\2\u047b\u047c\3\2\2\2\u047c\u047e\3\2\2\2\u047d\u047f\5\u00c8"+
		"e\2\u047e\u047d\3\2\2\2\u047e\u047f\3\2\2\2\u047f\u0480\3\2\2\2\u0480"+
		"\u0481\7}\2\2\u0481\u00c5\3\2\2\2\u0482\u0483\7R\2\2\u0483\u0484\7|\2"+
		"\2\u0484\u0485\5\u00c8e\2\u0485\u0486\7}\2\2\u0486\u048c\3\2\2\2\u0487"+
		"\u0488\7|\2\2\u0488\u0489\5\u00c8e\2\u0489\u048a\bd\1\2\u048a\u048c\3"+
		"\2\2\2\u048b\u0482\3\2\2\2\u048b\u0487\3\2\2\2\u048c\u00c7\3\2\2\2\u048d"+
		"\u0492\5\u00caf\2\u048e\u048f\7z\2\2\u048f\u0491\5\u00caf\2\u0490\u048e"+
		"\3\2\2\2\u0491\u0494\3\2\2\2\u0492\u0490\3\2\2\2\u0492\u0493\3\2\2\2\u0493"+
		"\u00c9\3\2\2\2\u0494\u0492\3\2\2\2\u0495\u0497\5\u014e\u00a8\2\u0496\u0498"+
		"\5\u00ccg\2\u0497\u0496\3\2\2\2\u0497\u0498\3\2\2\2\u0498\u00cb\3\2\2"+
		"\2\u0499\u049a\7|\2\2\u049a\u049b\7\u0098\2\2\u049b\u049c\7}\2\2\u049c"+
		"\u00cd\3\2\2\2\u049d\u049e\7^\2\2\u049e\u049f\7X\2\2\u049f\u04a0\5\u0144"+
		"\u00a3\2\u04a0\u00cf\3\2\2\2\u04a1\u04a2\7\t\2\2\u04a2\u04a3\7T\2\2\u04a3"+
		"\u04a4\5\u00c0a\2\u04a4\u04a5\5\u00d2j\2\u04a5\u00d1\3\2\2\2\u04a6\u04a9"+
		"\5\u00d4k\2\u04a7\u04a9\5\u00ceh\2\u04a8\u04a6\3\2\2\2\u04a8\u04a7\3\2"+
		"\2\2\u04a9\u00d3\3\2\2\2\u04aa\u04ae\7|\2\2\u04ab\u04af\5\u00d6l\2\u04ac"+
		"\u04af\5\u00d8m\2\u04ad\u04af\5\u00dan\2\u04ae\u04ab\3\2\2\2\u04ae\u04ac"+
		"\3\2\2\2\u04ae\u04ad\3\2\2\2\u04af\u04b8\3\2\2\2\u04b0\u04b4\7z\2\2\u04b1"+
		"\u04b5\5\u00d6l\2\u04b2\u04b5\5\u00d8m\2\u04b3\u04b5\5\u00dan\2\u04b4"+
		"\u04b1\3\2\2\2\u04b4\u04b2\3\2\2\2\u04b4\u04b3\3\2\2\2\u04b5\u04b7\3\2"+
		"\2\2\u04b6\u04b0\3\2\2\2\u04b7\u04ba\3\2\2\2\u04b8\u04b6\3\2\2\2\u04b8"+
		"\u04b9\3\2\2\2\u04b9\u04bb\3\2\2\2\u04ba\u04b8\3\2\2\2\u04bb\u04bc\7}"+
		"\2\2\u04bc\u00d5\3\2\2\2\u04bd\u04be\7\6\2\2\u04be\u04bf\5\u00dco\2\u04bf"+
		"\u04c1\5\u0090I\2\u04c0\u04c2\5\u0096L\2\u04c1\u04c0\3\2\2\2\u04c1\u04c2"+
		"\3\2\2\2\u04c2\u04c4\3\2\2\2\u04c3\u04c5\5\u0142\u00a2\2\u04c4\u04c3\3"+
		"\2\2\2\u04c4\u04c5\3\2\2\2\u04c5\u00d7\3\2\2\2\u04c6\u04c7\7\33\2\2\u04c7"+
		"\u04c8\5\u00dco\2\u04c8\u00d9\3\2\2\2\u04c9\u04ca\79\2\2\u04ca\u04cb\5"+
		"\u00dco\2\u04cb\u04cd\5\u0090I\2\u04cc\u04ce\5\u0096L\2\u04cd\u04cc\3"+
		"\2\2\2\u04cd\u04ce\3\2\2\2\u04ce\u04d0\3\2\2\2\u04cf\u04d1\5\u0142\u00a2"+
		"\2\u04d0\u04cf\3\2\2\2\u04d0\u04d1\3\2\2\2\u04d1\u00db\3\2\2\2\u04d2\u04d7"+
		"\5\u00dep\2\u04d3\u04d4\7\u0083\2\2\u04d4\u04d6\5\u00e0q\2\u04d5\u04d3"+
		"\3\2\2\2\u04d6\u04d9\3\2\2\2\u04d7\u04d5\3\2\2\2\u04d7\u04d8\3\2\2\2\u04d8"+
		"\u00dd\3\2\2\2\u04d9\u04d7\3\2\2\2\u04da\u04df\5\u014e\u00a8\2\u04db\u04dc"+
		"\7~\2\2\u04dc\u04de\7\177\2\2\u04dd\u04db\3\2\2\2\u04de\u04e1\3\2\2\2"+
		"\u04df\u04dd\3\2\2\2\u04df\u04e0\3\2\2\2\u04e0\u00df\3\2\2\2\u04e1\u04df"+
		"\3\2\2\2\u04e2\u04e7\5\u014e\u00a8\2\u04e3\u04e4\7~\2\2\u04e4\u04e6\7"+
		"\177\2\2\u04e5\u04e3\3\2\2\2\u04e6\u04e9\3\2\2\2\u04e7\u04e5\3\2\2\2\u04e7"+
		"\u04e8\3\2\2\2\u04e8\u04ee\3\2\2\2\u04e9\u04e7\3\2\2\2\u04ea\u04eb\7_"+
		"\2\2\u04eb\u04ec\7|\2\2\u04ec\u04ee\7}\2\2\u04ed\u04e2\3\2\2\2\u04ed\u04ea"+
		"\3\2\2\2\u04ee\u00e1\3\2\2\2\u04ef\u04f0\7\33\2\2\u04f0\u04f3\7T\2\2\u04f1"+
		"\u04f2\7,\2\2\u04f2\u04f4\7!\2\2\u04f3\u04f1\3\2\2\2\u04f3\u04f4\3\2\2"+
		"\2\u04f4\u04f5\3\2\2\2\u04f5\u04f6\5\u00c0a\2\u04f6\u00e3\3\2\2\2\u04f7"+
		"\u04f8\7\24\2\2\u04f8\u04fc\7-\2\2\u04f9\u04fa\7,\2\2\u04fa\u04fb\7;\2"+
		"\2\u04fb\u04fd\7!\2\2\u04fc\u04f9\3\2\2\2\u04fc\u04fd\3\2\2\2\u04fd\u04fe"+
		"\3\2\2\2\u04fe\u04ff\5\u00e6t\2\u04ff\u0500\7?\2\2\u0500\u0509\5\u00c0"+
		"a\2\u0501\u0502\7|\2\2\u0502\u0503\5\u00e8u\2\u0503\u0504\7}\2\2\u0504"+
		"\u050a\3\2\2\2\u0505\u0506\7|\2\2\u0506\u0507\5\u00e8u\2\u0507\u0508\b"+
		"s\1\2\u0508\u050a\3\2\2\2\u0509\u0501\3\2\2\2\u0509\u0505\3\2\2\2\u050a"+
		"\u050c\3\2\2\2\u050b\u050d\5\u0142\u00a2\2\u050c\u050b\3\2\2\2\u050c\u050d"+
		"\3\2\2\2\u050d\u00e5\3\2\2\2\u050e\u050f\5\u014e\u00a8\2\u050f\u00e7\3"+
		"\2\2\2\u0510\u0515\5\u00eav\2\u0511\u0512\7z\2\2\u0512\u0514\5\u00eav"+
		"\2\u0513\u0511\3\2\2\2\u0514\u0517\3\2\2\2\u0515\u0513\3\2\2\2\u0515\u0516"+
		"\3\2\2\2\u0516\u00e9\3\2\2\2\u0517\u0515\3\2\2\2\u0518\u051a\5\u00ba^"+
		"\2\u0519\u051b\5\u00f0y\2\u051a\u0519\3\2\2\2\u051a\u051b\3\2\2\2\u051b"+
		"\u0522\3\2\2\2\u051c\u0522\5\u00ecw\2\u051d\u051f\5\u00eex\2\u051e\u0520"+
		"\5\u00f0y\2\u051f\u051e\3\2\2\2\u051f\u0520\3\2\2\2\u0520\u0522\3\2\2"+
		"\2\u0521\u0518\3\2\2\2\u0521\u051c\3\2\2\2\u0521\u051d\3\2\2\2\u0522\u00eb"+
		"\3\2\2\2\u0523\u0524\5\u00ba^\2\u0524\u0525\7\u0083\2\2\u0525\u0526\7"+
		"\63\2\2\u0526\u0527\7|\2\2\u0527\u0528\7}\2\2\u0528\u0534\3\2\2\2\u0529"+
		"\u052a\7\62\2\2\u052a\u052b\7|\2\2\u052b\u052c\5\u00ba^\2\u052c\u052d"+
		"\7}\2\2\u052d\u0534\3\2\2\2\u052e\u052f\7\63\2\2\u052f\u0530\7|\2\2\u0530"+
		"\u0531\5\u00ba^\2\u0531\u0532\7}\2\2\u0532\u0534\3\2\2\2\u0533\u0523\3"+
		"\2\2\2\u0533\u0529\3\2\2\2\u0533\u052e\3\2\2\2\u0534\u00ed\3\2\2\2\u0535"+
		"\u0536\5\u00ba^\2\u0536\u0537\7\u0083\2\2\u0537\u0538\7_\2\2\u0538\u0539"+
		"\7|\2\2\u0539\u053a\7}\2\2\u053a\u0545\3\2\2\2\u053b\u053c\5\u00ba^\2"+
		"\u053c\u053d\7~\2\2\u053d\u053e\7\177\2\2\u053e\u0545\3\2\2\2\u053f\u0540"+
		"\7\34\2\2\u0540\u0541\7|\2\2\u0541\u0542\5\u00ba^\2\u0542\u0543\7}\2\2"+
		"\u0543\u0545\3\2\2\2\u0544\u0535\3\2\2\2\u0544\u053b\3\2\2\2\u0544\u053f"+
		"\3\2\2\2\u0545\u0548\3\2\2\2\u0546\u0547\7\u0083\2\2\u0547\u0549\5\u00ba"+
		"^\2\u0548\u0546\3\2\2\2\u0548\u0549\3\2\2\2\u0549\u00ef\3\2\2\2\u054a"+
		"\u054b\7\f\2\2\u054b\u054c\t\f\2\2\u054c\u00f1\3\2\2\2\u054d\u054e\7\24"+
		"\2\2\u054e\u054f\7\'\2\2\u054f\u0553\7-\2\2\u0550\u0551\7,\2\2\u0551\u0552"+
		"\7;\2\2\u0552\u0554\7!\2\2\u0553\u0550\3\2\2\2\u0553\u0554\3\2\2\2\u0554"+
		"\u0555\3\2\2\2\u0555\u0556\5\u00e6t\2\u0556\u0557\7?\2\2\u0557\u0558\5"+
		"\u00c0a\2\u0558\u055a\5\u00f4{\2\u0559\u055b\5\u00fa~\2\u055a\u0559\3"+
		"\2\2\2\u055a\u055b\3\2\2\2\u055b\u055d\3\2\2\2\u055c\u055e\7C\2\2\u055d"+
		"\u055c\3\2\2\2\u055d\u055e\3\2\2\2\u055e\u0560\3\2\2\2\u055f\u0561\5\u0142"+
		"\u00a2\2\u0560\u055f\3\2\2\2\u0560\u0561\3\2\2\2\u0561\u00f3\3\2\2\2\u0562"+
		"\u0563\7|\2\2\u0563\u0564\5\u00f6|\2\u0564\u0565\7}\2\2\u0565\u056b\3"+
		"\2\2\2\u0566\u0567\7|\2\2\u0567\u0568\5\u00f6|\2\u0568\u0569\b{\1\2\u0569"+
		"\u056b\3\2\2\2\u056a\u0562\3\2\2\2\u056a\u0566\3\2\2\2\u056b\u00f5\3\2"+
		"\2\2\u056c\u0571\5\u00f8}\2\u056d\u056e\7z\2\2\u056e\u0570\5\u00f8}\2"+
		"\u056f\u056d\3\2\2\2\u0570\u0573\3\2\2\2\u0571\u056f\3\2\2\2\u0571\u0572"+
		"\3\2\2\2\u0572\u00f7\3\2\2\2\u0573\u0571\3\2\2\2\u0574\u0576\5\u00eav"+
		"\2\u0575\u0577\5\u013a\u009e\2\u0576\u0575\3\2\2\2\u0576\u0577\3\2\2\2"+
		"\u0577\u00f9\3\2\2\2\u0578\u057c\5\u00fc\177\2\u0579\u057b\5\u00fc\177"+
		"\2\u057a\u0579\3\2\2\2\u057b\u057e\3\2\2\2\u057c\u057a\3\2\2\2\u057c\u057d"+
		"\3\2\2\2\u057d\u00fb\3\2\2\2\u057e\u057c\3\2\2\2\u057f\u0580\7\37\2\2"+
		"\u0580\u0581\7\u008a\2\2\u0581\u0586\7\u0098\2\2\u0582\u0583\7 \2\2\u0583"+
		"\u0584\7\u008a\2\2\u0584\u0586\7\u0098\2\2\u0585\u057f\3\2\2\2\u0585\u0582"+
		"\3\2\2\2\u0586\u00fd\3\2\2\2\u0587\u0588\7\33\2\2\u0588\u058b\7-\2\2\u0589"+
		"\u058a\7,\2\2\u058a\u058c\7!\2\2\u058b\u0589\3\2\2\2\u058b\u058c\3\2\2"+
		"\2\u058c\u058d\3\2\2\2\u058d\u058e\5\u00e6t\2\u058e\u058f\7?\2\2\u058f"+
		"\u0591\5\u00c0a\2\u0590\u0592\7C\2\2\u0591\u0590\3\2\2\2\u0591\u0592\3"+
		"\2\2\2\u0592\u00ff\3\2\2\2\u0593\u0596\t\r\2\2\u0594\u0595\7\f\2\2\u0595"+
		"\u0597\7\60\2\2\u0596\u0594\3\2\2\2\u0596\u0597\3\2\2\2\u0597\u05a9\3"+
		"\2\2\2\u0598\u0599\7T\2\2\u0599\u05a2\5\u00c0a\2\u059a\u059b\7|\2\2\u059b"+
		"\u059c\5\u0102\u0082\2\u059c\u059d\7}\2\2\u059d\u05a3\3\2\2\2\u059e\u059f"+
		"\7|\2\2\u059f\u05a0\5\u0102\u0082\2\u05a0\u05a1\b\u0081\1\2\u05a1\u05a3"+
		"\3\2\2\2\u05a2\u059a\3\2\2\2\u05a2\u059e\3\2\2\2\u05a2\u05a3\3\2\2\2\u05a3"+
		"\u05aa\3\2\2\2\u05a4\u05a5\7-\2\2\u05a5\u05a6\5\u00e6t\2\u05a6\u05a7\7"+
		"?\2\2\u05a7\u05a8\5\u00c0a\2\u05a8\u05aa\3\2\2\2\u05a9\u0598\3\2\2\2\u05a9"+
		"\u05a4\3\2\2\2\u05aa\u0101\3\2\2\2\u05ab\u05b0\5\u00dco\2\u05ac\u05ad"+
		"\7z\2\2\u05ad\u05af\5\u00dco\2\u05ae\u05ac\3\2\2\2\u05af\u05b2\3\2\2\2"+
		"\u05b0\u05ae\3\2\2\2\u05b0\u05b1\3\2\2\2\u05b1\u0103\3\2\2\2\u05b2\u05b0"+
		"\3\2\2\2\u05b3\u05b6\7S\2\2\u05b4\u05b5\7\f\2\2\u05b5\u05b7\7\60\2\2\u05b6"+
		"\u05b4\3\2\2\2\u05b6\u05b7\3\2\2\2\u05b7\u05c4\3\2\2\2\u05b8\u05c5\7U"+
		"\2\2\u05b9\u05c5\7]\2\2\u05ba\u05c5\7M\2\2\u05bb\u05bc\7\\\2\2\u05bc\u05c5"+
		"\5\u0114\u008b\2\u05bd\u05be\7L\2\2\u05be\u05c5\5\u014e\u00a8\2\u05bf"+
		"\u05c0\7.\2\2\u05c0\u05c1\7?\2\2\u05c1\u05c5\5\u00c0a\2\u05c2\u05c3\7"+
		"T\2\2\u05c3\u05c5\5\u00c0a\2\u05c4\u05b8\3\2\2\2\u05c4\u05b9\3\2\2\2\u05c4"+
		"\u05ba\3\2\2\2\u05c4\u05bb\3\2\2\2\u05c4\u05bd\3\2\2\2\u05c4\u05bf\3\2"+
		"\2\2\u05c4\u05c2\3\2\2\2\u05c5\u0105\3\2\2\2\u05c6\u05c7\7\24\2\2\u05c7"+
		"\u05c8\7\\\2\2\u05c8\u05ca\5\u0118\u008d\2\u05c9\u05cb\5\u0120\u0091\2"+
		"\u05ca\u05c9\3\2\2\2\u05ca\u05cb\3\2\2\2\u05cb\u05cd\3\2\2\2\u05cc\u05ce"+
		"\7\7\2\2\u05cd\u05cc\3\2\2\2\u05cd\u05ce\3\2\2\2\u05ce\u0107\3\2\2\2\u05cf"+
		"\u05d0\7\24\2\2\u05d0\u05d1\7L\2\2\u05d1\u05d2\5\u014e\u00a8\2\u05d2\u0109"+
		"\3\2\2\2\u05d3\u05d4\7\t\2\2\u05d4\u05d5\7\\\2\2\u05d5\u05d7\5\u0114\u008b"+
		"\2\u05d6\u05d8\5\u011e\u0090\2\u05d7\u05d6\3\2\2\2\u05d7\u05d8\3\2\2\2"+
		"\u05d8\u05da\3\2\2\2\u05d9\u05db\7f\2\2\u05da\u05d9\3\2\2\2\u05da\u05db"+
		"\3\2\2\2\u05db\u05dd\3\2\2\2\u05dc\u05de\7d\2\2\u05dd\u05dc\3\2\2\2\u05dd"+
		"\u05de\3\2\2\2\u05de\u05e0\3\2\2\2\u05df\u05e1\5\u011c\u008f\2\u05e0\u05df"+
		"\3\2\2\2\u05e0\u05e1\3\2\2\2\u05e1\u05e3\3\2\2\2\u05e2\u05e4\5\u0120\u0091"+
		"\2\u05e3\u05e2\3\2\2\2\u05e3\u05e4\3\2\2\2\u05e4\u010b\3\2\2\2\u05e5\u05e6"+
		"\7\33\2\2\u05e6\u05e7\7\\\2\2\u05e7\u05e9\5\u0114\u008b\2\u05e8\u05ea"+
		"\7\20\2\2\u05e9\u05e8\3\2\2\2\u05e9\u05ea\3\2\2\2\u05ea\u010d\3\2\2\2"+
		"\u05eb\u05ec\7\33\2\2\u05ec\u05ed\7L\2\2\u05ed\u05ee\5\u014e\u00a8\2\u05ee"+
		"\u010f\3\2\2\2\u05ef\u05f3\7(\2\2\u05f0\u05f4\5\u0122\u0092\2\u05f1\u05f4"+
		"\5\u0124\u0093\2\u05f2\u05f4\5\u0126\u0094\2\u05f3\u05f0\3\2\2\2\u05f3"+
		"\u05f1\3\2\2\2\u05f3\u05f2\3\2\2\2\u05f4\u0111\3\2\2\2\u05f5\u05f9\7K"+
		"\2\2\u05f6\u05fa\5\u0128\u0095\2\u05f7\u05fa\5\u012a\u0096\2\u05f8\u05fa"+
		"\5\u012c\u0097\2\u05f9\u05f6\3\2\2\2\u05f9\u05f7\3\2\2\2\u05f9\u05f8\3"+
		"\2\2\2\u05fa\u0113\3\2\2\2\u05fb\u05fe\5\u014e\u00a8\2\u05fc\u05fe\5\u014a"+
		"\u00a6\2\u05fd\u05fb\3\2\2\2\u05fd\u05fc\3\2\2\2\u05fe\u0115\3\2\2\2\u05ff"+
		"\u0600\7+\2\2\u0600\u0601\5\u011a\u008e\2\u0601\u0117\3\2\2\2\u0602\u0603"+
		"\5\u014e\u00a8\2\u0603\u0605\5\u0116\u008c\2\u0604\u0606\7d\2\2\u0605"+
		"\u0604\3\2\2\2\u0605\u0606\3\2\2\2\u0606\u0608\3\2\2\2\u0607\u0609\5\u011c"+
		"\u008f\2\u0608\u0607\3\2\2\2\u0608\u0609\3\2\2\2\u0609\u060e\3\2\2\2\u060a"+
		"\u060b\5\u014a\u00a6\2\u060b\u060c\7c\2\2\u060c\u060e\3\2\2\2\u060d\u0602"+
		"\3\2\2\2\u060d\u060a\3\2\2\2\u060e\u0119\3\2\2\2\u060f\u0610\7\16\2\2"+
		"\u0610\u0611\5\u014a\u00a6\2\u0611\u011b\3\2\2\2\u0612\u0613\7D\2\2\u0613"+
		"\u0614\7\65\2\2\u0614\u0615\5\u0144\u00a3\2\u0615\u011d\3\2\2\2\u0616"+
		"\u0618\5\u0116\u008c\2\u0617\u0619\7e\2\2\u0618\u0617\3\2\2\2\u0618\u0619"+
		"\3\2\2\2\u0619\u011f\3\2\2\2\u061a\u061b\7\5\2\2\u061b\u061c\t\16\2\2"+
		"\u061c\u0121\3\2\2\2\u061d\u061e\5\u014c\u00a7\2\u061e\u061f\7W\2\2\u061f"+
		"\u0620\5\u012e\u0098\2\u0620\u0123\3\2\2\2\u0621\u0622\5\u0130\u0099\2"+
		"\u0622\u0623\7W\2\2\u0623\u0624\5\u014e\u00a8\2\u0624\u0125\3\2\2\2\u0625"+
		"\u0626\5\u0134\u009b\2\u0626\u0627\7?\2\2\u0627\u0628\5\u0136\u009c\2"+
		"\u0628\u0629\7W\2\2\u0629\u062a\5\u014e\u00a8\2\u062a\u0127\3\2\2\2\u062b"+
		"\u062c\5\u014c\u00a7\2\u062c\u062d\7&\2\2\u062d\u062e\5\u012e\u0098\2"+
		"\u062e\u0129\3\2\2\2\u062f\u0630\5\u0130\u0099\2\u0630\u0631\7&\2\2\u0631"+
		"\u0632\5\u014e\u00a8\2\u0632\u012b\3\2\2\2\u0633\u0634\5\u0134\u009b\2"+
		"\u0634\u0635\7?\2\2\u0635\u0636\5\u0136\u009c\2\u0636\u0637\7&\2\2\u0637"+
		"\u0638\5\u014e\u00a8\2\u0638\u012d\3\2\2\2\u0639\u063a\7\\\2\2\u063a\u063e"+
		"\5\u0114\u008b\2\u063b\u063c\7L\2\2\u063c\u063e\5\u014e\u00a8\2\u063d"+
		"\u0639\3\2\2\2\u063d\u063b\3\2\2\2\u063e\u012f\3\2\2\2\u063f\u0644\5\u0132"+
		"\u009a\2\u0640\u0641\7z\2\2\u0641\u0643\5\u0132\u009a\2\u0642\u0640\3"+
		"\2\2\2\u0643\u0646\3\2\2\2\u0644\u0642\3\2\2\2\u0644\u0645\3\2\2\2\u0645"+
		"\u0131\3\2\2\2\u0646\u0644\3\2\2\2\u0647\u064a\5\u014e\u00a8\2\u0648\u064a"+
		"\7b\2\2\u0649\u0647\3\2\2\2\u0649\u0648\3\2\2\2\u064a\u0133\3\2\2\2\u064b"+
		"\u064e\5\u0132\u009a\2\u064c\u064e\7\b\2\2\u064d\u064b\3\2\2\2\u064d\u064c"+
		"\3\2\2\2\u064e\u0656\3\2\2\2\u064f\u0652\7z\2\2\u0650\u0653\5\u0132\u009a"+
		"\2\u0651\u0653\7\b\2\2\u0652\u0650\3\2\2\2\u0652\u0651\3\2\2\2\u0653\u0655"+
		"\3\2\2\2\u0654\u064f\3\2\2\2\u0655\u0658\3\2\2\2\u0656\u0654\3\2\2\2\u0656"+
		"\u0657\3\2\2\2\u0657\u0135\3\2\2\2\u0658\u0656\3\2\2\2\u0659\u065a\5\u00c0"+
		"a\2\u065a\u0137\3\2\2\2\u065b\u065e\5\u013a\u009e\2\u065c\u065e\5\u013c"+
		"\u009f\2\u065d\u065b\3\2\2\2\u065d\u065c\3\2\2\2\u065e\u0139\3\2\2\2\u065f"+
		"\u0660\7\u0080\2\2\u0660\u0665\5\u013e\u00a0\2\u0661\u0662\7z\2\2\u0662"+
		"\u0664\5\u013e\u00a0\2\u0663\u0661\3\2\2\2\u0664\u0667\3\2\2\2\u0665\u0663"+
		"\3\2\2\2\u0665\u0666\3\2\2\2\u0666\u0668\3\2\2\2\u0667\u0665\3\2\2\2\u0668"+
		"\u0669\7\u0081\2\2\u0669\u066d\3\2\2\2\u066a\u066b\7\u0080\2\2\u066b\u066d"+
		"\7\u0081\2\2\u066c\u065f\3\2\2\2\u066c\u066a\3\2\2\2\u066d\u013b\3\2\2"+
		"\2\u066e\u066f\7~\2\2\u066f\u0674\5\u0140\u00a1\2\u0670\u0671\7z\2\2\u0671"+
		"\u0673\5\u0140\u00a1\2\u0672\u0670\3\2\2\2\u0673\u0676\3\2\2\2\u0674\u0672"+
		"\3\2\2\2\u0674\u0675\3\2\2\2\u0675\u0677\3\2\2\2\u0676\u0674\3\2\2\2\u0677"+
		"\u0678\7\177\2\2\u0678\u067c\3\2\2\2\u0679\u067a\7~\2\2\u067a\u067c\7"+
		"\177\2\2\u067b\u066e\3\2\2\2\u067b\u0679\3\2\2\2\u067c\u013d\3\2\2\2\u067d"+
		"\u067e\7\u009b\2\2\u067e\u067f\7{\2\2\u067f\u0680\5\u0140\u00a1\2\u0680"+
		"\u013f\3\2\2\2\u0681\u0689\5\u013a\u009e\2\u0682\u0689\5\u013c\u009f\2"+
		"\u0683\u0689\7\u009b\2\2\u0684\u0689\5\u0148\u00a5\2\u0685\u0689\7\u0097"+
		"\2\2\u0686\u0689\7\u0096\2\2\u0687\u0689\7\u0095\2\2\u0688\u0681\3\2\2"+
		"\2\u0688\u0682\3\2\2\2\u0688\u0683\3\2\2\2\u0688\u0684\3\2\2\2\u0688\u0685"+
		"\3\2\2\2\u0688\u0686\3\2\2\2\u0688\u0687\3\2\2\2\u0689\u0141\3\2\2\2\u068a"+
		"\u068b\7\22\2\2\u068b\u068c\5\u014a\u00a6\2\u068c\u0143\3\2\2\2\u068d"+
		"\u068e\7\u0098\2\2\u068e\u068f\5\u0146\u00a4\2\u068f\u0145\3\2\2\2\u0690"+
		"\u0691\t\17\2\2\u0691\u0147\3\2\2\2\u0692\u0694\7\u0093\2\2\u0693\u0692"+
		"\3\2\2\2\u0693\u0694\3\2\2\2\u0694\u0695\3\2\2\2\u0695\u0696\t\20\2\2"+
		"\u0696\u0149\3\2\2\2\u0697\u0698\t\21\2\2\u0698\u014b\3\2\2\2\u0699\u069e"+
		"\5\u014e\u00a8\2\u069a\u069b\7z\2\2\u069b\u069d\5\u014e\u00a8\2\u069c"+
		"\u069a\3\2\2\2\u069d\u06a0\3\2\2\2\u069e\u069c\3\2\2\2\u069e\u069f\3\2"+
		"\2\2\u069f\u014d\3\2\2\2\u06a0\u069e\3\2\2\2\u06a1\u06a5\t\22\2\2\u06a2"+
		"\u06a3\7\u009f\2\2\u06a3\u06a5\b\u00a8\1\2\u06a4\u06a1\3\2\2\2\u06a4\u06a2"+
		"\3\2\2\2\u06a5\u014f\3\2\2\2\u00b9\u0164\u0167\u0173\u0181\u0184\u0187"+
		"\u018a\u018d\u0192\u0197\u019d\u01a9\u01b0\u01b9\u01c1\u01c7\u01cb\u01ce"+
		"\u01d1\u01d4\u01df\u01ea\u01ed\u01f3\u01fe\u0213\u0216\u021a\u0226\u022a"+
		"\u022e\u0237\u0248\u0253\u0257\u025e\u0261\u0266\u026e\u0272\u0276\u027b"+
		"\u0280\u0288\u028c\u0296\u029d\u02a5\u02ab\u02b0\u02b2\u02b8\u02bf\u02c4"+
		"\u02ca\u02ce\u02d2\u02d8\u02e8\u02ed\u02f4\u02f8\u02fc\u0302\u0312\u0319"+
		"\u032b\u032e\u0343\u0348\u035f\u0364\u0367\u036e\u0374\u037e\u0382\u038a"+
		"\u038e\u0396\u039a\u03a3\u03ad\u03b0\u03b8\u03c7\u03cf\u03e0\u03e9\u03f1"+
		"\u03f4\u03f8\u03fc\u03fe\u0406\u0427\u042f\u0435\u0444\u044c\u0451\u0458"+
		"\u045c\u0462\u0468\u046d\u0471\u0479\u047b\u047e\u048b\u0492\u0497\u04a8"+
		"\u04ae\u04b4\u04b8\u04c1\u04c4\u04cd\u04d0\u04d7\u04df\u04e7\u04ed\u04f3"+
		"\u04fc\u0509\u050c\u0515\u051a\u051f\u0521\u0533\u0544\u0548\u0553\u055a"+
		"\u055d\u0560\u056a\u0571\u0576\u057c\u0585\u058b\u0591\u0596\u05a2\u05a9"+
		"\u05b0\u05b6\u05c4\u05ca\u05cd\u05d7\u05da\u05dd\u05e0\u05e3\u05e9\u05f3"+
		"\u05f9\u05fd\u0605\u0608\u060d\u0618\u063d\u0644\u0649\u064d\u0652\u0656"+
		"\u065d\u0665\u066c\u0674\u067b\u0688\u0693\u069e\u06a4";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
