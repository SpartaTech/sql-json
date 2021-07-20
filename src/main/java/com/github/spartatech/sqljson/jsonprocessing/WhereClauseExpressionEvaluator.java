package com.github.spartatech.sqljson.jsonprocessing;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.spartatech.sqljson.exception.ExpressionNotSupportedException;
import com.github.spartatech.sqljson.vo.ExpressionSidesValidator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.spartatech.sqljson.exception.ExceptionWrapper;
import com.github.spartatech.sqljson.vo.MultiColumn;
import com.github.spartatech.sqljson.vo.MultiListType;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.github.spartatech.sqljson.util.GeneralConverters.*;
import static com.github.spartatech.sqljson.util.StringUtility.unquote;

public class WhereClauseExpressionEvaluator extends ExpressionVisitorAdapter {

    private static final Logger log = LoggerFactory.getLogger(WhereClauseExpressionEvaluator.class);

    private final JsonNode element;
    private boolean keep;

    public boolean isKeep() {
        return keep;
    }

    public WhereClauseExpressionEvaluator(JsonNode element) {
        this.element = element;
    }

    @Override
    public void visit(BitwiseRightShift bitwiseRightShift) {
        throw ExpressionNotSupportedException.fromExpression("bitwiseRightShift");
    }

    @Override
    public void visit(BitwiseLeftShift bitwiseLeftShift) {
        throw ExpressionNotSupportedException.fromExpression("bitwiseLeftShift");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw ExpressionNotSupportedException.fromExpression("jdbcParameter");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw ExpressionNotSupportedException.fromExpression("jdbcNamedParameter");
    }

    @Override
    public void visit(HexValue hexValue) {
        throw ExpressionNotSupportedException.fromExpression("hexValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        final WhereClauseExpressionEvaluator ex = new WhereClauseExpressionEvaluator(element);
        parenthesis.getExpression().accept(ex);
        this.keep = ex.keep;
    }

    @Override
    public void visit(AndExpression andExpression) {
        final WhereClauseExpressionEvaluator left = new WhereClauseExpressionEvaluator(element);
        final WhereClauseExpressionEvaluator right = new WhereClauseExpressionEvaluator(element);
        andExpression.getLeftExpression().accept(left);
        andExpression.getRightExpression().accept(right);
        keep = left.isKeep() && right.isKeep();
    }

    @Override
    public void visit(OrExpression orExpression) {
        final WhereClauseExpressionEvaluator left = new WhereClauseExpressionEvaluator(element);
        final WhereClauseExpressionEvaluator right = new WhereClauseExpressionEvaluator(element);
        orExpression.getLeftExpression().accept(left);
        orExpression.getRightExpression().accept(right);
        keep = left.isKeep() || right.isKeep();
    }

    @Override
    public void visit(Between between) {
        try {
            final Object field = resolveValue(between.getLeftExpression());
            final Object startRange = resolveValue(between.getBetweenExpressionStart());
            final Object endRange = resolveValue(between.getBetweenExpressionEnd());

            final ExpressionSidesValidator validator = item-> {
                if (!isNumeric(item)) {
                    throw new SQLException("cannot perform between for " + item.getClass().getSimpleName());
                }
                Object convertedItem = item;
                Object convertedStartRange = startRange;
                Object convertedEndRange = endRange;
                if ((item instanceof Double) || startRange instanceof Double || endRange instanceof Double) {
                    convertedItem = convertToDouble(item);
                    convertedStartRange = convertToDouble(startRange);
                    convertedEndRange = convertToDouble(endRange);
                }
                validateMatchingComparisonExpressions(convertedItem, convertedStartRange, "Between(start range)");
                validateMatchingComparisonExpressions(convertedItem, convertedEndRange, "Between(end range)");
            };

            final Predicate<Object> comparison = item ->  {

                if ((item instanceof Double) || startRange instanceof Double || endRange instanceof Double) {
                    Double convertedItem = convertToDouble(item);
                    return convertedItem  >= convertToDouble(startRange) && convertedItem <= convertToDouble(endRange);
                } else {
                    return (Long)item >= (Long)startRange && (Long)item <= (Long)endRange;
                }
            };

            if (field instanceof MultiColumn) {
                keep = processMultiColumn((MultiColumn) field, validator, comparison);
            } else {
                validator.validate(field);
                keep = comparison.test(field);
            }
        } catch (SQLException e) {
            throw ExceptionWrapper.of(e);
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        compareExpressions(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), (l, r) -> l.equals(r));
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        compareExpressions(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), (l ,r) ->
            numericComparison(l, r , NumericOperation.GREATER_THAN)
        );
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        compareExpressions(greaterThanEquals.getLeftExpression(), greaterThanEquals.getRightExpression(), (l ,r) ->
            numericComparison(l, r, NumericOperation.GREATER_THAN_EQUAL));
    }

    @Override
    public void visit(InExpression inExpression) {
        log.debug("Evaluating inExpression: {}", inExpression.toString());
        final List<Object> left = new ArrayList<>();
        final List<Object> right = new ArrayList<>();

        try {
            if (inExpression.getLeftExpression() != null) {
                final Object resolved = resolveValue(inExpression.getLeftExpression());
                log.trace("Left side is Expression: [{}], resolved to: [{}]", inExpression.getLeftExpression(), resolved);
                left.add(resolved);
            } else if (inExpression.getLeftItemsList() != null) {
                final InListItemEvaluator inListEval = new InListItemEvaluator();
                inExpression.getLeftItemsList().accept(inListEval);
                left.addAll(inListEval.result);
                log.trace("Left side is Item List [{}], resolved to: [{}]", inExpression.getLeftExpression(), inListEval.result);
            }

            if (inExpression.getRightExpression() != null) {
                final Object resolved = resolveValue(inExpression.getRightExpression());
                log.trace("Right side is Expression: [{}], resolved to: [{}]", inExpression.getLeftExpression(), resolved);
                if (resolved instanceof List) {
                    right.addAll((List) resolved);
                } else {
                    right.add(resolved);
                }
            } else if (inExpression.getRightItemsList() != null) {
                final InListItemEvaluator inListEval = new InListItemEvaluator();
                inExpression.getRightItemsList().accept(inListEval);
                right.addAll(inListEval.result);
                log.trace("Right side is Item List [{}], resolved to: [{}]", inExpression.getLeftExpression(), inListEval.result);
            }

            if (left.isEmpty() || right.isEmpty()) {
                log.debug("both sides of expression are null [{}], eliminating response", inExpression.toString());
                keep = false;
                return;
            }

            if (left.size() == 1 && left.get(0) instanceof MultiColumn) {
                log.trace("Left side is MultiColumn [{}]", left.get(0));
                final MultiColumn multi = ((MultiColumn) left.get(0));
                log.trace("MultiColumn {} evaluation left: [{}], right=[{}]", multi.getFilterType(), left, right);
                if (multi.getFilterType() == MultiListType.MATCH_ALL) {
                    keep = multi.getItems().stream().allMatch(right::contains);
                } else {
                    keep = multi.getItems().stream().anyMatch(right::contains);
                }
                log.trace("Expression evaluated to: {}", keep);
            } else {
                log.trace("not Multicolumn");
                if(!left.get(0).getClass().equals(right.get(0).getClass())) {
                    log.trace("IN list mismatch {} != {}", left.get(0).getClass().getSimpleName() , right.get(0).getClass().getSimpleName());
                    throw new SQLException("In list mismatch: "
                            + left.get(0).getClass().getSimpleName() + ", " + right.get(0).getClass().getSimpleName());
                }

                keep = right.containsAll(left);
                log.trace("Normal in evaluation result: {}", keep);
            }
            if (inExpression.isNot()) {
                log.trace("it is not in reversing result: {}", !keep);
                keep = !keep;
            }

        } catch (SQLException e) {
            throw ExceptionWrapper.of(e);
        }
    }


    @Override
    public void visit(FullTextSearch fullTextSearch) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {

    }

    @Override
    public void visit(MinorThan minorThan) {
        compareExpressions(minorThan.getLeftExpression(), minorThan.getRightExpression(), (l ,r) ->
                numericComparison(l, r , NumericOperation.LESS_THAN)
        );
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        compareExpressions(minorThanEquals.getLeftExpression(), minorThanEquals.getRightExpression(), (l ,r) ->
                numericComparison(l, r , NumericOperation.LESS_THAN_EQUAL)
        );
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        compareExpressions(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(), (l, r) -> !l.equals(r));
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new ExpressionNotSupportedException("SubSelect");
    }

    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }

    @Override
    public void visit(CastExpression castExpression) {

    }

    @Override
    public void visit(Modulo modulo) {

    }

    @Override
    public void visit(AnalyticExpression analyticExpression) {

    }

    @Override
    public void visit(ExtractExpression extractExpression) {

    }

    @Override
    public void visit(IntervalExpression intervalExpression) {

    }

    @Override
    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

    }

    @Override
    public void visit(RegExpMatchOperator regExpMatchOperator) {

    }

    @Override
    public void visit(JsonExpression jsonExpression) {

    }

    @Override
    public void visit(JsonOperator jsonOperator) {

    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {

    }

    @Override
    public void visit(UserVariable userVariable) {

    }

    @Override
    public void visit(NumericBind numericBind) {

    }

    @Override
    public void visit(KeepExpression keepExpression) {

    }

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {

    }

    @Override
    public void visit(ValueListExpression valueListExpression) {

    }

    @Override
    public void visit(RowConstructor rowConstructor) {

    }

    @Override
    public void visit(OracleHint oracleHint) {

    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {

    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

    }

    @Override
    public void visit(NotExpression notExpression) {
        notExpression.getExpression().accept(this);
        keep = !keep;
    }

    @Override
    public void visit(NextValExpression nextValExpression) {
        throw ExpressionNotSupportedException.fromExpression("NextVal not supported");
    }

    @Override
    public void visit(CollateExpression collateExpression) {
        throw ExpressionNotSupportedException.fromExpression("Collate not supported");
    }

    @Override
    public void visit(SimilarToExpression similarToExpression) {

    }

    @Override
    public void visit(ArrayExpression arrayExpression) {

    }

    @Override
    public void visit(VariableAssignment variableAssignment) {

    }

    @Override
    public void visit(XMLSerializeExpr xmlSerializeExpr) {

    }

    /**
     * This method compare two expression, which could possibly be MultiColumn.
     * The comparison is done using the predicate, which delegates to caller.
     *
     * @param leftExpression left Expression
     * @param rightExpression Right Expression
     * @param comparison predicate to compare
     */
    private void compareExpressions(Expression leftExpression, Expression rightExpression, BiPredicate<Object, Object> comparison) {
        try {
            final Object left = resolveValue(leftExpression);
            final Object right = resolveValue(rightExpression);

            if ((left instanceof MultiColumn) && (right instanceof MultiColumn)) {
                throw new SQLException("MultiColumn on both sides of expression not allowed.");
            }
            if ((left instanceof MultiColumn) || (right instanceof MultiColumn)) {
                final MultiColumn multi;
                final Object otherSide;
                if (left instanceof MultiColumn) {
                    multi = (MultiColumn) left;
                    otherSide = right;
                } else {
                    multi = (MultiColumn) right;
                    otherSide = left;
                }
                keep = processMultiColumn(multi,
                        item -> validateMatchingComparisonExpressions(item, otherSide, "="),
                        item ->comparison.test(item, otherSide));
            } else {
                keep = comparison.test(left, right);
            }
        } catch (SQLException e) {
            throw ExceptionWrapper.of(e);
        }
    }
    
    private boolean processMultiColumn(MultiColumn multi, ExpressionSidesValidator validator, Predicate<Object> comparison) {
        final boolean result;
        if (multi.getFilterType() == MultiListType.MATCH_ANY) {
            result = multi.getItems().stream()
                    .peek(item -> {
                        try {
                            validator.validate(item);
                        } catch (SQLException e) {
                            throw ExceptionWrapper.of(e);
                        }
                    })
                    .anyMatch(comparison);
        } else {
            result = multi.getItems().stream()
                    .peek(item -> {
                        try {
                            validator.validate(item);
                        } catch (SQLException e) {
                            throw ExceptionWrapper.of(e);
                        }
                    })
                    .allMatch(comparison);
        }

        return result;
    }


    /**
     * Resolve Value as it's Original type.
     *
     * @param value Expression to be resolved
     * @return original value
     * @throws SQLException in case cannot resolve it.
     */
    private Object resolveValue(Expression value) throws SQLException {
        if (value instanceof TimestampValue) {
            return ((TimestampValue) value).getValue().toLocalDateTime();
        }
        if (value instanceof DateValue) {
            return ((DateValue) value).getValue().toLocalDate();
        }
        if (value instanceof TimeValue) {
            return ((TimeValue) value).getValue().toLocalTime();
        }
        if (value instanceof StringValue) {
            return ((StringValue) value).getValue();
        }
        if (value instanceof LongValue) {
            return ((LongValue) value).getValue();
        }
        if (value instanceof DoubleValue) {
            return ((DoubleValue)value).getValue();
        }
        if (value instanceof NullValue) {
            return null;
        }
        if (value instanceof Column) {
            final String column = ((Column) value).getFullyQualifiedName();
            JsonNode result = element;
            for(String node : column.split("\\.")) {
                result = result.path(node);
                if (result == null) {
                    throw new SQLException("Column not found "+ column);
                }
            }

            return convertJsonNodeToNative(result);
        }
        if (value instanceof Function) {
            //TODO this is just a workaround to accept filter on json arrays,
            //TODO We need to proper implement functions
            final Function func = (Function) value;
            final String funcName = func.getName();
            final MultiColumn resultColumn;
            try {
                resultColumn = new MultiColumn(MultiListType.fromValue(funcName));
            } catch (EnumConstantNotPresentException e) {
                throw new SQLException("Function " + funcName + " currently not supported. Only supported ones are " + MultiListType.allOptionsAsString());
            }

            if (func.getParameters().getExpressions().size() != 1) {
                throw new SQLException("Invalid number of parameters. "+ funcName + " expects 1 parameter, but got " + func.getParameters().getExpressions().size());
            }

            if (!(func.getParameters().getExpressions().get(0) instanceof Column)) {
                throw new SQLException("Parameters for function "+ funcName + " should be column");
            }

            final String column = ((Column) func.getParameters().getExpressions().get(0)).getFullyQualifiedName();
            final List<Object> results = parseMultiColumn(element, column, column.split("\\."));
            resultColumn.addItems(results);
            return resultColumn;
        }

        final ArithmeticExpressionEvaluator innerEval = new ArithmeticExpressionEvaluator();
        value.accept(innerEval);
        return innerEval.result;
    }

    /**
     * This method parses special columns called MultiColumn.
     * A multi column is a column that derives from Json Array. so it can be multiple value.
     * MultiColumns only allowed with the use of functions matchAny or matchAll so we know what kind of comparison user
     * wants.
     *
     * This is a recursive function that traverses through columnPath.
     *
     * @param element element to traverse
     * @param fullPath FullPath for Logging and Exception information only
     * @param columnPath current path, as it recursively find elements.
     * @return List of objects found.
     * @throws SQLException In case a problem happens in the parsing.
     */
    private List<Object> parseMultiColumn(JsonNode element, String fullPath, String... columnPath) throws SQLException {
        final List<Object> result = new ArrayList<>();
        if (element.isArray()) {
            for (JsonNode item : element) {
                result.addAll(parseMultiColumn(item, fullPath, columnPath));
            }
        } else {
            if (columnPath.length == 0) {
                result.add(convertJsonNodeToNative(element));
            } else {
                final JsonNode nextEl = element.path(columnPath[0]);
                if (nextEl.isMissingNode()) {
                    throw new SQLException("Column not found "+ fullPath);
                }
                result.addAll(parseMultiColumn(nextEl, fullPath, Arrays.copyOfRange(columnPath, 1, columnPath.length)));
            }
        }
        return result;
    }

    /**
     * Converts value from JsonNode to native Java Object.
     * @param node JsonNode to be converted
     * @return Java native Object converted
     */
    private Object convertJsonNodeToNative(JsonNode node) {
        if (node.isArray()) {
            return StreamSupport
                    .stream(node.spliterator(), false)
                    .map(this::convertJsonNodeToNative)
                    .collect(Collectors.toList());
        }
        if (node.isInt() || node.isLong()) {
            return node.asLong();
        } else if (node.isFloat() || node.isDouble()) {
            return node.asDouble();
        } else {
            final String unquoted = unquote(node.toString());
            final Optional<Instant> instant = convertTextToInstant(unquoted);
            if (instant.isPresent()) {
                return instant.get();
            } else {
                final Optional<LocalDateTime> dateTime = convertTextToLocalDateTime(unquoted);
                if (dateTime.isPresent()) {
                    return dateTime.get();
                } else {
                    final Optional<LocalDate> date = convertTextToLocalDate(unquoted);
                    if (date.isPresent()) {
                        return date.get();
                    } else {
                        final Optional<LocalTime> time = convertTextToLocalTime(unquoted);
                        if (time.isPresent()) {
                            return date.get();
                        } else {
                            return unquoted;
                        }
                    }
                }
            }
        }
    }

    /**
     * Verifies if a given object is considered numeric.
     * For numeric comparisons.
     *
     * @param obj object to be checked
     * @return true -> is numeric, false -> is not numeric
     */
    private boolean isNumeric(Object obj) {
        if ((obj instanceof Double)
                || obj instanceof Long) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Does numeric comparison for operands different than equal.
     *
     * @param left left side for comparison
     * @param right right side for comparison
     * @param op operation to be performed
     */
    private boolean numericComparison (Object left, Object right, NumericOperation op) {
        boolean returnValue;
        try {
            validateMatchingComparisonExpressions(left, right, op.getSymbol());

            if (left instanceof Long && right instanceof Long) {
                switch (op) {
                    case GREATER_THAN: returnValue = ((long)left) > (long) right; break;
                    case GREATER_THAN_EQUAL: returnValue = ((long)left) >= (long) right; break;
                    case LESS_THAN: returnValue = ((long)left) < (long) right; break;
                    case LESS_THAN_EQUAL: returnValue = ((long)left) <= (long) right; break;
                    default: throw new SQLException("Operation " + op.getSymbol() + " not implemented");
                }
            } else if (left instanceof Double && right instanceof Double) {
                switch (op) {
                    case GREATER_THAN: returnValue = ((double)left) > (double) right; break;
                    case GREATER_THAN_EQUAL: returnValue = ((double)left) >= (double) right; break;
                    case LESS_THAN: returnValue = ((double)left) < (double) right; break;
                    case LESS_THAN_EQUAL: returnValue = ((double)left) <= (double) right; break;
                    default: throw new SQLException("Operation " + op.getSymbol() + " not implemented");
                }
            } else {
                throw new SQLException(left.getClass().getSimpleName() + " not supported for " + op.getSymbol());
            }
        } catch (SQLException e) {
            throw ExceptionWrapper.of(e);
        }
        return returnValue;
    }

    /**
     * Enum with all possible Numeric operations.
     */
    enum NumericOperation {
        //Comparison
        GREATER_THAN(">"),
        GREATER_THAN_EQUAL(">="),
        LESS_THAN("<"),
        LESS_THAN_EQUAL("<="),
        BETWEEN("between"),

        //Arithmetic
        ADDITION("+"),
        SUBTRACTION("-"),
        DIVISION("/"),
        MULTIPLICATION("*")
        ;

        final String symbol;

        NumericOperation(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }
    }

    /**
     * Validates if two sides of the comparison matches by type.
     * @param left to validate
     * @param right to validate
     * @param operand to generate error message in case needed
     * @throws SQLException in comparison is not allowed.
     */
    private void validateMatchingComparisonExpressions (Object left, Object right, String operand) throws SQLException {
        if (left == null || right == null || left.getClass() != right.getClass()) {
            final String leftType = left == null ? "" : "("+left.getClass().getSimpleName()+ ") ";
            final String rightType = right == null ? "" :  "("+right.getClass().getSimpleName()+ ")";
            throw new SQLException(
                    "Invalid types in expression " + left + leftType + operand + " " + right + rightType);
        }
    }

    /**
     * This sub-class is specialized in Arithmetic Expression calculations.
     */
    private class ArithmeticExpressionEvaluator extends ExpressionVisitorAdapter {

        private Object result;

        @Override
        public void visit(Addition expr) {
            numericOperation(expr.getLeftExpression(), expr.getRightExpression(), NumericOperation.ADDITION);
        }

        @Override
        public void visit(Division expr) {
            numericOperation(expr.getLeftExpression(), expr.getRightExpression(), NumericOperation.DIVISION);
        }

        @Override
        public void visit(Multiplication expr) {
            numericOperation(expr.getLeftExpression(), expr.getRightExpression(), NumericOperation.MULTIPLICATION);
        }

        @Override
        public void visit(Subtraction expr) {
            numericOperation(expr.getLeftExpression(), expr.getRightExpression(), NumericOperation.SUBTRACTION);
        }


        /**
         * Does numeric operation.
         *
         * @param leftExpression left side for operation
         * @param rightExpression right side for operation
         * @param op operation to be performed
         */
        private void numericOperation (Expression leftExpression, Expression rightExpression, NumericOperation op) {
            try {
                final Object left = resolveValue(leftExpression);
                final Object right = resolveValue(rightExpression);

                validateMatchingArithmeticExpressions(left, right, op.getSymbol());

                if (left instanceof Double && right instanceof Double) {
                    switch (op) {
                        case ADDITION: result = ((double) left) + (double) right; break;
                        case SUBTRACTION: result = ((double) left) - (double) right; break;
                        case DIVISION: result = ((double) left) / (double) right; break;
                        case MULTIPLICATION: result = ((double) left) * (double) right; break;
                        default: throw new SQLException("Operation " + op.getSymbol() + " not implemented");
                    }
                } else if (left instanceof Long && right instanceof Long) {
                    switch (op) {
                        case ADDITION: result = ((long) left) + (long) right; break;
                        case SUBTRACTION: result = ((long) left) - (long) right; break;
                        case DIVISION: result = ((long) left) / (long) right; break;
                        case MULTIPLICATION: result = ((long) left) * (long) right; break;
                        default: throw new SQLException("Operation " + op.getSymbol() + " not implemented");
                    }
                } else if (left instanceof Long && right instanceof Double) {
                    switch (op) {
                        case ADDITION: result = ((long) left) + (double) right; break;
                        case SUBTRACTION: result = ((long) left) - (double) right; break;
                        case DIVISION: result = ((long) left) / (double) right; break;
                        case MULTIPLICATION: result = ((long) left) * (double) right; break;
                        default: throw new SQLException("Operation " + op.getSymbol() + " not implemented");
                    }
                } else if (left instanceof Double && right instanceof Long) {
                    switch (op) {
                        case ADDITION: result = ((double) left) + (long) right; break;
                        case SUBTRACTION: result = ((double) left) - (long) right; break;
                        case DIVISION: result = ((double) left) / (long) right; break;
                        case MULTIPLICATION: result = ((double) left) * (long) right; break;
                        default: throw new SQLException("Operation " + op.getSymbol() + " not implemented");
                    }
                }

            } catch (SQLException e) {
                throw ExceptionWrapper.of(e);
            }
        }

        private void validateMatchingArithmeticExpressions (Object left, Object right, String operand) throws SQLException{
            final String leftType = "("+ (left == null ? "null" : left.getClass().getSimpleName())+ ") ";
            final String rightType = "("+(right == null ? "null" :  "("+right.getClass().getSimpleName())+ ")";

            if (left == null || !((left instanceof Long) || (left instanceof Double))) {
                throw new SQLException(
                        leftType + " is invalid type in expression " + left + leftType + operand + " " + right + rightType);
            }
            if (right == null || !((right instanceof Long) || (right instanceof Double))) {
                throw new SQLException(
                        rightType + " is invalid type in expression " + left + leftType + operand + " " + right + rightType);
            }
        }
    }

    class InListItemEvaluator implements ItemsListVisitor {
        final List<Object> result = new ArrayList<>();

        @Override
        public void visit(SubSelect subSelect) {
            throw new ExpressionNotSupportedException("SubSelect in list not supported");
        }

        @Override
        public void visit(ExpressionList expressionList) {
            expressionList.getExpressions().forEach(expr -> {
                try {
                    final Object resolved = resolveValue(expr);
                    if (!result.isEmpty() && !result.get(0).getClass().equals(resolved.getClass())) {
                        throw new SQLException("Different types in list: "
                                + result.get(0).getClass().getSimpleName() + "!=" + resolved.getClass().getSimpleName());
                    }
                    result.add(resolved);
                } catch (SQLException e) {
                    throw ExceptionWrapper.of(e);
                }
            });
        }

        @Override
        public void visit(MultiExpressionList multiExpressionList) {
            throw new ExpressionNotSupportedException("MultiExpression in list not supported");
        }

        @Override
        public void visit(NamedExpressionList namedExpressionList) {
            throw new ExpressionNotSupportedException("NamedExpression in list not supported");
        }


    }
}
