package com.psddev.dari.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PredicateParserTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredicateParserTest.class);

    private static final String REFLECTIVE_IDENTITY_SYNTAX_STANDARD = "?";
    private static final String REFLECTIVE_IDENTITY_DELIMITED_SYNTAX = "?/";
    private static final String REFLECTIVE_IDENTITY_REDUNDANT_SYNTAX = "?_id";
    private static final String REFLECTIVE_IDENTITY_DELIMITED_REDUNDANT_SYNTAX = "?/_id";
    private static final String REFLECTIVE_IDENTITY_INDEXED_SYNTAX = "?0";
    private static final String REFLECTIVE_IDENTITY_INDEXED_DELIMITED_SYNTAX = "?0/";
    private static final String REFLECTIVE_IDENTITY_INDEXED_REDUNDANT_SYNTAX = "?0_id";
    private static final String REFLECTIVE_IDENTITY_INDEXED_DELIMITED_REDUNDANT_SYNTAX = "?0/_id";

    private static List<TestDatabase> TEST_DATABASES;
    private static List<Database> DATABASES;

	PredicateParser parser = new PredicateParser();
	static final Queue<Object> EMPTY_OBJECT_QUEUE = new ArrayDeque<Object>();
	static final Queue<String> EMPTY_STRING_QUEUE = new ArrayDeque<String>();

    @BeforeClass
    public static void beforeClass() {

        TEST_DATABASES = DatabaseTestUtils.getNewDefaultTestDatabaseInstances();
        DATABASES = new ArrayList<Database>();

        for (TestDatabase testDb : TEST_DATABASES) {
            Database db = testDb.get();
            DATABASES.add(db);
        }

        LOGGER.info("Running tests against " + TEST_DATABASES.size() + " databases.");
    }

    @AfterClass
    public static void afterClass() {
        if (TEST_DATABASES != null) for (TestDatabase testDb : TEST_DATABASES) {
            testDb.close();
        }
    }

    @Before
    public void before() {

    }

    @After
    public void after() {

    }

    /**
     * public Predicate parse(String predicateString, Object... parameters)
     */
    @Test
    public void parse_empty() {
    	assertEquals(null, parser.parse(""));
    }
    @Test
    public void parse_empty_2() {
    	assertEquals(null, parser.parse(" "));
    }

    @Test
    public void parse_null() {
    	assertEquals(null, parser.parse(null));
    }

    @Test
    public void parse_simple() {
    	Predicate pred = parser.parse("a = 1");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_nospace() {
    	parser.parse("a=1");
    }

    @Test
    public void parse_compound() {
    	Predicate pred = parser.parse("a = 1 and b != 2");
    	Predicate expect = CompoundPredicate.combine(
    	        PredicateParser.AND_OPERATOR,
    			new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1")),
                new ComparisonPredicate(PredicateParser.NOT_EQUALS_ALL_OPERATOR, false, "b", Arrays.asList("2"))
    	);
    	assertEquals(expect, pred);
    }

    @Test // left to right
    public void parse_noparens() {
    	Predicate pred = parser.parse("a = 1 and b = 2 or c = 3");
    	Predicate expect = CompoundPredicate.combine(
    	        PredicateParser.OR_OPERATOR,
    			CompoundPredicate.combine(
    			        PredicateParser.AND_OPERATOR,
    					new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1")),
    					new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("2"))
    			),
    			new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "c", Arrays.asList("3"))
    	);
    	assertEquals(expect, pred);
    }

    @Test // to group
    public void parse_parens() {
    	Predicate pred = parser.parse("a = 1 and (b = 2 or c = 3)");
    	Predicate expect = CompoundPredicate.combine(
    	        PredicateParser.AND_OPERATOR,
				new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1")),
    			CompoundPredicate.combine(
    			        PredicateParser.OR_OPERATOR,
    					new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("2")),
    	    			new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "c", Arrays.asList("3"))
    			)
    	);
    	assertEquals(expect, pred);
    }

    /*
     * Key handling
     */
    @Test
    public void parse_key_canonical_id() {
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "_id", Arrays.asList("1"));
    	assertEquals(expect, parser.parse("id = 1"));
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_key_missing() {
    	parser.parse(" = 1 ");
    }

    /*
     * Comparison handling
     */
    @Test
    public void parse_operator_negative() {
    	Predicate expect = new ComparisonPredicate(PredicateParser.NOT_EQUALS_ALL_OPERATOR, false, "token1", Arrays.asList("1"));
    	assertEquals(expect, parser.parse("token1 != 1"));
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_operator_missing() {
    	parser.parse("a 1");
    }

    /*
     * Value handling
     */
    @Test (expected=IllegalArgumentException.class)
    public void parse_error_value_missing() {
    	parser.parse("a = ");
    }

    @Test
    public void parse_value_boolean_true() {
    	ComparisonPredicate expect = new ComparisonPredicate(
    	        PredicateParser.EQUALS_ANY_OPERATOR,
    			false,
    			"token1",
    			Arrays.asList(true));
    	assertEquals(expect, parser.parse("token1 = true"));
    }

    @Test
    public void parse_value_boolean_false() {
    	Predicate expect = new ComparisonPredicate(
    	        PredicateParser.EQUALS_ANY_OPERATOR,
    			false,
    			"token1",
    			Arrays.asList(false));
    	assertEquals(expect, parser.parse("token1 = false"));
    }

    @Test
    public void parse_value_null() {
    	Predicate expect = new ComparisonPredicate(
    	        PredicateParser.EQUALS_ANY_OPERATOR,
    			false,
    			"token1",
    			null);
    	assertEquals(expect, parser.parse("token1 = null"));
    }

    /*
     * Params
     */
    @Test
    public void parse_params() {
    	Predicate pred = parser.parse("b = ?", "1");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_params_multivalue() {
    	Predicate pred = parser.parse("a in ?", Arrays.asList(1,2,3));
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList(1,2,3));
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_params_notstring() {
    	Predicate pred = parser.parse("b = ?", 1);
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList(1));
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_params_null() {
    	Predicate pred = parser.parse("b = ?", (Object)null);
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", null);
    	assertEquals(expect, pred);
    }
    @Test
    public void parse_params_missing() {
    	Predicate pred = parser.parse("b = ?");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", null);
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_extra() {
    	Predicate pred = parser.parse("b = ?", "1", "2");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_parens_empty() {
    	assertEquals(null, parser.parse(" ( ) "));
    }

    /** evaluate reflective identity "?" syntax **/

    public void evaluate_reflective_syntax(String reflectiveSyntax) {
        for (Database database : DATABASES) {

            ReflectiveTestRecord current = ReflectiveTestRecord.getInstance(database);
            ReflectiveTestRecord other = ReflectiveTestRecord.getInstance(database);

            current.setOther(other);
            other.setOther(current);

            assertTrue("\"" + reflectiveSyntax + "\" was evaluated incorrectly!", PredicateParser.Static.evaluate(other, "other = " + reflectiveSyntax, current));
        }
    }

    public void evaluate_reflective_syntax_state_valued(String reflectiveSyntax) {
        for (Database database : DATABASES) {

            ReflectiveTestRecord current = ReflectiveTestRecord.getInstance(database);
            ReflectiveTestRecord other = ReflectiveTestRecord.getInstance(database);

            current.setOther(other);
            other.setOther(current);

            assertTrue("\"" + reflectiveSyntax + "\" was evaluated incorrectly!", PredicateParser.Static.evaluate(other, "other = " + reflectiveSyntax, current.getState()));
        }
    }

    @Test
    public void evaluate_reflective_identity_syntax_standard() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_SYNTAX_STANDARD);
    }

    @Test
    public void evaluate_reflective_identity_syntax_standard_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_SYNTAX_STANDARD);
    }

    /** evaluate reflective identity delimited "?/" syntax **/

    @Test
    @Ignore
    public void evaluate_reflective_identity_delimited_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_DELIMITED_SYNTAX);
    }

    @Test
    @Ignore
    public void evaluate_reflective_identity_delimited_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_DELIMITED_SYNTAX);
    }

    /** evaluate reflective identity redundant "?_id" syntax **/

    @Test
    public void evaluate_reflective_identity_redundant_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_REDUNDANT_SYNTAX);
    }

    @Test
    public void evaluate_reflective_identity_redundant_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_REDUNDANT_SYNTAX);
    }

    /** evaluate reflective identity delimited redundant "?/_id" syntax **/

    @Test
    @Ignore
    public void evaluate_reflective_identity_delimited_redundant_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_DELIMITED_REDUNDANT_SYNTAX);
    }

    @Test
    @Ignore
    public void evaluate_reflective_identity_delimited_redundant_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_DELIMITED_REDUNDANT_SYNTAX);
    }

    /** evaluate reflective identity indexed "?0" syntax **/

    @Test
    public void evaluate_reflective_identity_indexed_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_INDEXED_SYNTAX);
    }

    @Test
    public void evaluate_reflective_identity_indexed_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_INDEXED_SYNTAX);
    }

    /** evaluate reflective identity indexed delimited "?0/" syntax **/

    @Test
    public void evaluate_reflective_identity_indexed_delimited_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_SYNTAX);
    }

    @Test
    public void evaluate_reflective_identity_indexed_delimited_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_SYNTAX);
    }

    /** evaluate reflective identity indexed redundant "?0_id" syntax **/

    @Test
    @Ignore
    public void evaluate_reflective_identity_indexed_redundant_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_INDEXED_REDUNDANT_SYNTAX);
    }

    @Test
    @Ignore
    public void evaluate_reflective_identity_indexed_redundant_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_INDEXED_REDUNDANT_SYNTAX);
    }

    /** evaluate reflective identity indexed delimited redundant "?0/_id" syntax **/

    @Test
    public void evaluate_reflective_identity_indexed_delimited_redundant_syntax() {

        evaluate_reflective_syntax(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_REDUNDANT_SYNTAX);
    }

    @Test
    public void evaluate_reflective_identity_indexed_delimited_redundant_syntax_state_valued() {

        evaluate_reflective_syntax_state_valued(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_REDUNDANT_SYNTAX);
    }

    public static class ReflectiveTestRecord extends Record {

        public static ReflectiveTestRecord getInstance(Database database) {

            ReflectiveTestRecord object = new ReflectiveTestRecord();
            object.getState().setDatabase(database);
            return object;
        }

        private ReflectiveTestRecord other;

        public ReflectiveTestRecord getOther() {
            return other;
        }

        public void setOther(ReflectiveTestRecord other) {
            this.other = other;
        }
    }

    /*
     * Options // TODO: Add tests here when we know what this should look like
     */

}
