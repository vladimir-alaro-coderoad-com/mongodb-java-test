package com.tierconnect.riot.api.database.mongo;

import com.tierconnect.riot.api.database.base.DataBase;
import com.tierconnect.riot.api.database.base.GenericDataBase;
import com.tierconnect.riot.api.database.base.GenericOperator;
import com.tierconnect.riot.api.database.base.Operation;
import com.tierconnect.riot.api.database.base.alias.Alias;
import com.tierconnect.riot.api.database.base.conditions.BooleanCondition;
import com.tierconnect.riot.api.database.base.conditions.ConditionBuilder;
import com.tierconnect.riot.api.database.base.operator.MultipleOperator;
import com.tierconnect.riot.api.database.base.operator.SingleOperator;
import com.tierconnect.riot.api.database.base.operator.SubQueryOperator;
import com.tierconnect.riot.api.database.base.operator.SubQueryOperatorAggregate;
import com.tierconnect.riot.api.database.base.ordination.Order;
import com.tierconnect.riot.api.database.exception.OperationNotImplementedException;
import com.tierconnect.riot.api.database.exception.OperationNotSupportedException;
import com.tierconnect.riot.api.database.mongo.aggregate.MongoMatch;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import com.tierconnect.riot.api.mongoShell.MongoDataBase;
import com.tierconnect.riot.api.mongoShell.ResultQuery;
import com.tierconnect.riot.api.mongoShell.operations.OperationExecutor;
import com.tierconnect.riot.api.mongoShell.query.QueryBuilder;
import com.tierconnect.riot.api.mongoShell.query.ResultFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import static com.tierconnect.riot.api.mongoShell.utils.CharacterUtils.*;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.join;

/**
 * Project: reporting-api
 * Author: edwin
 * Date: 28/11/2016
 */
public class Mongo extends DataBase<String> implements GenericDataBase {

    private static Logger logger = LogManager.getLogger(Mongo.class);

    private static final String MONGO_AND_CONDITIONS = "\"$and\"";
    private static final String MONGO_OR_CONDITIONS = "\"$or\"";
    private static final String MONGO_EQUALS = "\"$eq\"";
    private static final String MONGO_NOT_EQUALS = "\"$ne\"";
    private static final String MONGO_GREATER_THAN = "\"$gt\"";
    private static final String MONGO_LESS_THAN = "\"$lt\"";
    private static final String MONGO_GREATER_THAN_OR_EQUALS = "\"$gte\"";
    private static final String MONGO_LESS_THAN_OR_EQUALS = "\"$lte\"";
    private static final String MONGO_REGEX = "\"$regex\"";
    private static final String MONGO_IN = "\"$in\"";
    private static final String MONGO_NOT_IN = "\"$nin\"";
    private static final String MONGO_EXISTS = "\"$exists\"";
    private static final String MONGO_SIZE = "\"$size\"";
    private static final String MONGO_OPTIONS = "\"$options\"";
    private static final String MONGO_NOT = "\"$not\"";
    private static final String MONGO_ELEMENT_MATCH = "\"$elemMatch\"";

    private OperationExecutor operationExecutor;
    private QueryBuilder queryBuilder;

    //
    private String projectionString;
    private String aliasListString;
    private String sortString;
    private String filterString;

    // aggregate object
    private int limit = 1000000;
    private String aggregateString;
    private List<Pipeline> pipelineList;

    private String fileName;
    private String comment;
    private boolean enableExplain;
    private String withIndex;


    public Mongo(String connection, ConditionBuilder builder) {
        super(builder);
        this.operationExecutor = new OperationExecutor(connection);
        this.queryBuilder = new QueryBuilder(this.operationExecutor);
    }

    public void executeAggregate(String collection, List<Pipeline> groupByList) throws Exception {
        executeAggregate(collection, groupByList, false);
    }

    public void executeAggregate(String collection, List<Pipeline> groupByList, boolean enableExplain)
            throws Exception {
        prepareObjects(groupByList);
        logger.info("\n COLLECTION: " + collection + " \n AGGREGATE: " + aggregateString);
        long start = System.currentTimeMillis();
        ResultQuery execute = new MongoDataBase(this.operationExecutor)
                .getCollection(collection)
                .aggregate(aggregateString)
                .setExplain(enableExplain).execute();
        if (execute.getTotal() < 0 && execute.getErrorMessage() != null) {
            throw new IllegalArgumentException(execute.getErrorMessage().getErrorMessage());
        }
        resultSet = execute.getRows();
        countAll = (long) execute.getRows().size();
        executionPlan = execute.getExecutionPlan();
        logger.info("RUN QUERY OF SIZE [" + countAll + "} WITH TIME [" + (System.currentTimeMillis() - start) + "] ms");
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit) throws Exception {
        executeFind(collection, listAliasFind, skip, limit, null, "result.json", "",
                false, null, null, ExecutionResultScope.INCLUDE_RESULT_AND_TOTAL);
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit, Map<String, Order>
            orderMap, String comment, String withIndex) throws Exception {
        executeFind(collection, listAliasFind, skip, limit, orderMap, "result.json", comment, false, withIndex, null,
                ExecutionResultScope.INCLUDE_RESULT_AND_TOTAL);
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit, Map<String, Order>
            orderMap, String fileName, String comment, String withIndex, ExecutionResultScope resultScope) throws Exception {
        executeFind(collection, listAliasFind, skip, limit, orderMap, fileName, comment, false, withIndex, null,
                resultScope);
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit,
                            Map<String, Order> orderMap, String withIndex, ExecutorService executor, ExecutionResultScope resultScope) throws Exception {
        executeFind(collection, listAliasFind, skip, limit, orderMap, "result.json", "", false, withIndex, executor, resultScope);
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit, int maxTotalRecords, Map<String, Order>
            orderMap, String fileName, String comment, String withIndex, ExecutionResultScope resultScope) throws Exception {
        executeFind(collection, listAliasFind, skip, limit, maxTotalRecords, orderMap, fileName, comment, false, withIndex, null,
                resultScope);
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit, Map<String, Order>
            orderMap, String fileName, String comment, boolean enableExplain, String withIndex,
                            ExecutorService executor, ExecutionResultScope resultScope) throws Exception {
        executeFind(collection, listAliasFind, skip, limit, null, orderMap, fileName, comment, enableExplain, withIndex, executor, resultScope);
    }

    public void executeFind(String collection, List<Alias> listAliasFind, int skip, int limit, Integer maxTotalRecords, Map<String, Order>
            orderMap, String fileName, String comment, boolean enableExplain, String withIndex,
                            ExecutorService executor, ExecutionResultScope resultScope) throws Exception {
        prepareObjects(collection, listAliasFind, skip, limit, maxTotalRecords, orderMap, fileName, comment, enableExplain, withIndex, executor, resultScope);
        if (!resultScope.equals(ExecutionResultScope.INCLUDE_NOTHING)) {
            try {
                logger.info("\n    EXECUTE: " + resultScope
                        + "\n COLLECTION: " + collection + " \n QUERY:" + filterString
                        + "\n PROJECTION:" + projectionString
                        + (StringUtils.isNotEmpty(sortString) ? "\n SORT: " + sortString : StringUtils.EMPTY)
                        + "\n SKIP: " + skip + " LIMIT: " + limit
                        + (StringUtils.isNotEmpty(withIndex) ? "\n INDEX: " + withIndex : StringUtils.EMPTY));
                long start = System.currentTimeMillis();
                List<CompletableFuture<Void>> parallelComputation = new LinkedList<>();
                if (includeResult()) {
                    if (maxTotalRecords == null || (maxTotalRecords != null && skip < maxTotalRecords.intValue())) {
                        if (executor != null) {
                            parallelComputation.add(CompletableFuture.runAsync(this::executeFindQuery, executor));
                        } else {
                            parallelComputation.add(CompletableFuture.runAsync(this::executeFindQuery));
                        }
                    }
                }

                if (includeTotal()) {
                    if (executor != null) {
                        parallelComputation.add(CompletableFuture.runAsync(this::executeCountForFindQuery, executor));
                    } else {
                        parallelComputation.add(CompletableFuture.runAsync(this::executeCountForFindQuery));
                    }
                }

                parallelComputation.parallelStream()
                        .map(CompletableFuture::join)
                        .count();//do nothing special, just join;
                logger.info("RUN QUERY WITH TIME [" + (System.currentTimeMillis() - start) + "] ms");
            } catch (CompletionException ce) {
                handleException(ce);
            }
        }
    }

    public void executeFind(String collection, String query, String projection, String comment, String fileName) {
        executeFindQuery(collection, query, projection, comment, fileName);
    }

    private void handleException(CompletionException ce) throws Exception {
        Throwable cause = ce.getCause();
        cause = (cause instanceof CompletionException) ? cause.getCause() : cause;
        throw (Exception) cause.getCause();
    }

    private void executeFindQuery() {
        try {
            long start = System.currentTimeMillis();
            ResultQuery resultQuery = queryBuilder.find(filterString, projectionString, comment,
                    (fileName == null ? StringUtils.EMPTY : fileName) + Thread.currentThread().getName() + "Q_" +
                            UUID.nameUUIDFromBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
                    enableExplain, withIndex, true, false);
            resultSet = resultQuery.getRows();
            executionPlan = resultQuery.getExecutionPlan();
            logger.info("Query execution time: " + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void executeFindQuery(String collection, String query, String projection, String comment, String fileName) {
        try {
            long start = System.currentTimeMillis();
            queryBuilder.setCollection(collection);
            ResultQuery resultQuery = queryBuilder.find(query, projection, comment,
                    (fileName == null ? StringUtils.EMPTY : fileName) + Thread.currentThread().getName() + "Q_" +
                            UUID.nameUUIDFromBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
                    enableExplain, withIndex, true, false);
            resultSet = resultQuery.getRows();
            logger.info("\nQuery execution time: " + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void executeCountForFindQuery() {
        try {
            long start = System.currentTimeMillis();
            ResultQuery resultQuery = queryBuilder.find(filterString, projectionString, comment,
                    (fileName == null ? StringUtils.EMPTY : fileName) + Thread.currentThread().getName() + "C_" +
                            UUID.nameUUIDFromBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
                    enableExplain, withIndex, false, true);
            countAll = (long) resultQuery.getTotal();
            logger.info("\nCount execution time: " + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * return path of the file
     *
     * @param collection    {@link String} name collection in mongo
     * @param listAliasFind {@link List}<{@link Alias}> list Alias name
     * @param tmpFileName   {@link String} path + filename
     * @param resultFormat  {@link ResultFormat} type export values={BSON,JSON,CSV}
     * @return download path
     * @throws IOException
     * @throws InterruptedException
     * @throws OperationNotSupportedException
     */
    public String export(String collection, List<Alias> listAliasFind, final String tmpFileName,
                         final ResultFormat resultFormat, String comment)
            throws IOException, InterruptedException, OperationNotSupportedException {
        prepareObjects(collection, listAliasFind, null, null, null, null,
                null, false, null, null, ExecutionResultScope.INCLUDE_RESULT_AND_TOTAL);
        logger.debug("execute export with filter: " + filterString + " and with projection: " + projectionString);
        return queryBuilder.export(filterString, projectionString, aliasListString, tmpFileName, comment, resultFormat);
    }

    public String export(String script, final String tmpFileName, final ResultFormat resultFormat)
            throws IOException, InterruptedException {
        logger.debug("execute export with script: \n" + script);
        return queryBuilder.export(script, tmpFileName, resultFormat);
    }


    @Override
    public String transformMultiOperator(MultipleOperator operator) throws OperationNotSupportedException {
        return betweenBraces(
                converterKeyValueToString(String.valueOf(convertBooleanCondition(operator.getBooleanOperator())),
                        betweenBrackets(join(transformMultiOperatorList(operator.getGenericOperatorList()), COMMA))));
    }

    @Override
    public String transformSingleOperator(SingleOperator operator) throws OperationNotSupportedException {
        return betweenBraces(transformSingleOperatorWithoutBraces(operator)) + "\n";
    }

    @Override
    public String transformSubQueryOperator(SubQueryOperator operator) throws OperationNotSupportedException {
        // This subQuery as Mongo object shouldn't execute queries, only for prepare objects
        Mongo subQuery = new Mongo("", operator.getCondition());
        subQuery.prepareObjects(operator.getTarget(), null, null, null, null,
                null, null, false, null, null, ExecutionResultScope.INCLUDE_RESULT_AND_TOTAL);
        StringBuilder targetSubQuery = new StringBuilder();
        targetSubQuery.append("db.getCollection(\"")
                .append(operator.getTarget())
                .append("\")")
                .append(".find")
                .append(OPEN_PARENTHESIS)
                .append(subQuery.filterString);

        if (operator.getProjectionFilter() != null) {
            targetSubQuery.append(COMMA)
                    .append(formatFilterToProjectionString(operator.getProjectionFilter()));
        }
        targetSubQuery.append(CLOSE_PARENTHESIS)
                .append(".map(function(_paramToProject){ return ")
                .append("_paramToProject.")
                .append(operator.getFieldToProject())
                .append("})");
        StringBuilder builder = new StringBuilder(operator.getKeyWithQuote()).append(COLON).append(OPEN_BRACE);
        if (Operation.OperationEnum.IN.equals(operator.getOperator())) {
            builder.append(converterKeyValueToString(MONGO_IN, targetSubQuery.toString())).append(CLOSE_BRACE);
        } else {
            builder.append(converterKeyValueToString(MONGO_NOT_IN, targetSubQuery.toString())).append(CLOSE_BRACE);
        }
        return betweenBraces(builder.toString());
    }

    @Override
    public String transformSubQueryOperatorAggregate(SubQueryOperatorAggregate operator) throws OperationNotSupportedException {
        // This subQuery as Mongo object shouldn't execute queries, only for prepare objects
        Mongo subquery = new Mongo("", operator.getCondition());
        subquery.prepareObjects(operator.getPipelines());
        StringBuilder targetSubQuery = new StringBuilder();
        targetSubQuery.append("db.getCollection(\"")
                .append(operator.getTarget())
                .append("\")")
                .append(".aggregate")
                .append(OPEN_PARENTHESIS)
                .append(subquery.aggregateString)
//                .append(",{allowDiskUse:true}")
                .append(CLOSE_PARENTHESIS)
                .append(".map(function(_paramToProject){ return ")
                .append("_paramToProject.")
                .append(operator.getFieldToProject())
                .append("})");
        StringBuilder builder = new StringBuilder(operator.getKeyWithQuote()).append(COLON).append(OPEN_BRACE);
        if (Operation.OperationEnum.IN.equals(operator.getOperator())) {
            builder.append(converterKeyValueToString(MONGO_IN, targetSubQuery.toString())).append(CLOSE_BRACE);
        } else {
            builder.append(converterKeyValueToString(MONGO_NOT_IN, targetSubQuery.toString())).append(CLOSE_BRACE);
        }
        return betweenBraces(builder.toString());
    }

    @SuppressWarnings("unchecked")
    public String transformSingleOperatorWithoutBraces(SingleOperator operator) throws OperationNotSupportedException {
        StringBuilder builder = new StringBuilder(operator.getKeyWithQuote()).append(COLON);//.append(OPEN_BRACE);
        List<Object> listValues;
        if (Operation.OperationEnum.EQUALS.equals(operator.getOperator())) {
            builder.append(formatObject(operator.getValue()));
        } else {
            builder = new StringBuilder(operator.getKeyWithQuote()).append(COLON).append(OPEN_BRACE);
            if (Operation.OperationEnum.NOT_EQUALS.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_NOT_EQUALS, formatObject(operator.getValue()))).append
                        (CLOSE_BRACE);
            } else if (Operation.OperationEnum.GREATER_THAN.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_GREATER_THAN, formatObject(operator.getValue())))
                        .append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.LESS_THAN.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_LESS_THAN, formatObject(operator.getValue()))).append
                        (CLOSE_BRACE);
            } else if (Operation.OperationEnum.GREATER_THAN_OR_EQUALS.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_GREATER_THAN_OR_EQUALS, formatObject(operator.getValue
                        ()))).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.LESS_THAN_OR_EQUALS.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_LESS_THAN_OR_EQUALS, formatObject(operator.getValue())
                )).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.CONTAINS.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_REGEX, "/" + operator.getValue() + "/")).append
                        (CLOSE_BRACE);
            } else if (Operation.OperationEnum.STARTS_WITH.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_REGEX, "/^" + operator.getValue() + "?/")).append
                        (CLOSE_BRACE);
            } else if (Operation.OperationEnum.ENDS_WITH.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_REGEX, "/.*" + operator.getValue() + "\\\\\\b/"))
                        .append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.IN.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_IN, formatObject(operator.getValue()))).append
                        (CLOSE_BRACE);
            } else if (Operation.OperationEnum.NOT_IN.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_NOT_IN, formatObject(operator.getValue()))).append
                        (CLOSE_BRACE);
            } else if (Operation.OperationEnum.BETWEEN.equals(operator.getOperator())) {
                listValues = (List<Object>) operator.getValue();
                builder.append(converterKeyValueToString(MONGO_GREATER_THAN_OR_EQUALS, formatObject(listValues.get(0)
                ) + "")).append(COMMA);
                builder.append(converterKeyValueToString(MONGO_LESS_THAN_OR_EQUALS, formatObject(listValues.get(1)) +
                        "")).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.EXISTS.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_EXISTS, "true")).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.NOT_EXISTS.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_EXISTS, "false")).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.EMPTY.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_EQUALS, QUOTE + QUOTE)).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.NOT_EMPTY.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_NOT_EQUALS, QUOTE + QUOTE)).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.IS_NULL.equals(operator.getOperator())) {
                builder = new StringBuilder(MONGO_AND_CONDITIONS).append(COLON).append(OPEN_BRACKET);
                builder.append(betweenBraces(converterKeyValueToString(operator.getKeyWithQuote(), betweenBraces
                        (converterKeyValueToString(MONGO_EXISTS, "true")))));
                builder.append(COMMA);
                builder.append(betweenBraces(converterKeyValueToString(operator.getKeyWithQuote(), betweenBraces
                        (converterKeyValueToString(MONGO_EQUALS, "null")))));
                builder.append(CLOSE_BRACKET);
            } else if (Operation.OperationEnum.IS_NOT_NULL.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_NOT_EQUALS, "null")).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.EMPTY_ARRAY.equals(operator.getOperator())) {
                builder = new StringBuilder(MONGO_OR_CONDITIONS).append(COLON).append(OPEN_BRACKET);
                builder.append(betweenBraces(converterKeyValueToString(operator.getKeyWithQuote(), betweenBraces
                        (converterKeyValueToString(MONGO_EXISTS, "false")))));
                builder.append(COMMA);
                builder.append(betweenBraces(converterKeyValueToString(operator.getKeyWithQuote(), "null")));
                builder.append(COMMA);
                builder.append(betweenBraces(converterKeyValueToString(operator.getKeyWithQuote(), betweenBraces
                        (converterKeyValueToString(MONGO_SIZE, 0)))));
                builder.append(CLOSE_BRACKET);
            } else if (Operation.OperationEnum.NOT_EMPTY_ARRAY.equals(operator.getOperator())) {
                builder = new StringBuilder(operator.getKeyWithQuote()).append(COLON).append(OPEN_BRACE);
                builder.append(converterKeyValueToString(MONGO_EXISTS, true)).append(COMMA);
                builder.append(converterKeyValueToString(MONGO_NOT, betweenBraces(converterKeyValueToString
                        (MONGO_SIZE, 0))));
                builder.append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.ARRAY_SIZE_MATCH.equals(operator.getOperator())) {
                builder.append(converterKeyValueToString(MONGO_SIZE, operator.getValue() + "")).append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.REGEX.equals(operator.getOperator())) {
                listValues = (List<Object>) operator.getValue();
                builder.append(converterKeyValueToString(MONGO_REGEX, QUOTE + listValues.get(0) + QUOTE));
                builder.append(COMMA);
                builder.append(converterKeyValueToString(MONGO_OPTIONS, QUOTE + listValues.get(1) + QUOTE));
                builder.append(CLOSE_BRACE);
            } else if (Operation.OperationEnum.ELEMENT_MATCH.equals(operator.getOperator())) {
                builder.append(MONGO_ELEMENT_MATCH).append(COLON);
                builder.append(betweenBraces(join(elementMatchList((List<GenericOperator>) operator.getValue()),
                        COMMA)));
                builder.append(CLOSE_BRACE);
            } else {
                throw new OperationNotSupportedException(operator.getOperator() + " Operation not supported in Mongo");
            }
        }
        return builder.toString();
    }

    private List<String> elementMatchList(List<GenericOperator> operationList) throws OperationNotSupportedException {
        List<String> stringListOperator = new ArrayList<>(operationList.size());
        for (GenericOperator operation : operationList) {
            if (operation.isMultipleOperator()) {
                logger.error("operation ELEMENT MATCH is not implemented as Multiple Operator, " + operation);
                throw new OperationNotImplementedException("operation ELEMENT MATCH is not implemented as Multiple " +
                        "Operator");
            }
            stringListOperator.add(transformSingleOperatorWithoutBraces((SingleOperator) operation));
        }
        return stringListOperator;
    }


    private String convertBooleanCondition(BooleanCondition booleanCondition) {
        return (BooleanCondition.AND.equals(booleanCondition) ? MONGO_AND_CONDITIONS : MONGO_OR_CONDITIONS);
    }

    private String formatObject(Object object) {
        if (object instanceof String) {
            return transformObjectString(object);
        } else if (object instanceof Collection && !((Collection) object).isEmpty()) {
            return OPEN_BRACKET + formatList((List) object) + CLOSE_BRACKET;
        } else if (object instanceof Date) {
            return "ISODate(\"" + ISODateTimeFormat.dateTime().print(((Date) object).getTime()) + "\")";
        }
        return object.toString();
    }

    private String formatList(List listObject) {
        Object first = listObject.get(0);
        if (listObject.size() == 1) {
            return transformObjectString(first);
        }
        StringBuilder builder = new StringBuilder();
        builder.append(transformObjectString(first));
        for (int i = 1; i < listObject.size(); i++) {
            builder.append(COMMA);
            builder.append(transformObjectString(listObject.get(i)));
            if (i % 24 == 0) {
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    private String transformObjectString(Object element) {
        if (element instanceof String && !element.toString().startsWith("ObjectId(\"")) {
            return betweenDoubleQuote(element.toString());
        }
        return String.valueOf(element);
    }

    private void prepareObjects(List<Pipeline> groupByList) throws OperationNotSupportedException {
        this.pipelineList = groupByList;
        this.aggregateString = formatAggregate();
    }

    private void prepareObjects(String collection, List<Alias> listAliasFind, Integer skip, Integer limit,
                                Map<String, Order> orderMap,
                                String fileName, String comment, boolean enableExplain, String withIndex,
                                ExecutorService executor, ExecutionResultScope resultScope) throws OperationNotSupportedException {
        prepareObjects(collection, listAliasFind, skip, limit, null, orderMap, fileName, comment, enableExplain, withIndex, executor, resultScope);
    }

    private void prepareObjects(String collection, List<Alias> listAliasFind, Integer skip, Integer limit, Integer maxTotalRecords,
                                Map<String, Order> orderMap,
                                String fileName, String comment, boolean enableExplain, String withIndex,
                                ExecutorService executor, ExecutionResultScope resultScope) throws OperationNotSupportedException {
        queryBuilder.setCollection(collection);
        if (skip != null) {
            queryBuilder.skip(skip);
        }
        if (limit != null) {
            queryBuilder.limit(limit);
        }
        if (maxTotalRecords != null) {
            queryBuilder.maxTotalRecords(maxTotalRecords);
        }
        filterString = formatFilterString();
        setMapOrder(orderMap);
        setAliasList(listAliasFind);
        if (executor != null) {
            this.executor = executor;
        }
        if (resultScope == null) {
            throw new IllegalArgumentException("ResultScope could not be null for async requests");
        }
        this.resultScope = resultScope;
        this.fileName = fileName;
        this.comment = comment;
        this.enableExplain = enableExplain;
        this.withIndex = withIndex;
    }

    @Override
    public String getConditionBuilderString() throws OperationNotSupportedException {
        return formatFilterString();
    }

    @Override
    public void setMapOrder(Map<String, Order> mapOrder) {
        super.setMapOrder(mapOrder);
        formatSort();
    }

    @Override
    public void setAliasList(List<Alias> aliasList) {
        super.setAliasList(aliasList);
        formatProjection();
        formatAlias();
    }


    public String formatFilterString() throws OperationNotSupportedException {
        List<String> builderList = transformMultiOperatorList(builder.getListGenericOperator());
        if (builderList.isEmpty()) {
            return betweenBraces(EMPTY);
        }
        return betweenBraces(converterKeyValueToString(convertBooleanCondition(builder.getBooleanCondition()),
                betweenBrackets(join(builderList, COMMA))));
    }

    private String formatFilterToProjectionString(ConditionBuilder conditionBuilder) throws
            OperationNotSupportedException {
        List<String> builderList = transformMultiOperatorList(conditionBuilder.getListGenericOperator());
        if (builderList.isEmpty()) {
            return betweenBraces(EMPTY);
        }
        return join(builderList, COMMA);
    }

    private void formatSort() {
        sortString = null;
        if (mapOrder != null && !mapOrder.isEmpty()) {
            List<String> sortList = new ArrayList<>(mapOrder.size());
            for (Map.Entry<String, Order> orderEntry : mapOrder.entrySet()) {
                sortList.add(converterKeyValueToString(betweenDoubleQuote(orderEntry.getKey()), (Order.ASC.equals
                        (orderEntry.getValue()) ? "1" : "-1")));
            }
            sortString = betweenBraces(join(sortList, COMMA));
            queryBuilder.sort(sortString);
        }
    }

    private void formatProjection() {
        projectionString = betweenBraces(EMPTY);
        if (getAliasList() != null && !getAliasList().isEmpty()) {
            List<String> listProjectionString = new ArrayList<>(getAliasList().size());
            for (Alias alias : getAliasList()) {
                String value = converterKeyValueToString(betweenDoubleQuote(alias.getProperty()), alias.isExclude() ?
                        "0" : "1");
                if (!listProjectionString.contains(value)) {
                    listProjectionString.add(value);
                }
            }
            projectionString = betweenBraces(join(listProjectionString, COMMA + "\n"));
        }
    }

    private void formatAlias() {
        String formatAlias = "\"%1s\":{\"alias\":\"%2s\",\"function\":\"%3s\"}";
        aliasListString = betweenBraces(String.format(formatAlias, "_id", "_id", "none"));
        if (getAliasList() != null && !getAliasList().isEmpty()) {
            List<String> listAliasString = new ArrayList<>(getAliasList().size());
            for (Alias alias : getAliasList()) {
                if (!alias.isExclude()) {
                    if (alias.getFunction() == null) {
                        listAliasString.add(String.format(formatAlias, alias.getProperty(), alias.getLabel(), "none"));
                    } else {
                        listAliasString.add(String.format(formatAlias, alias.getProperty(), alias.getLabel(), alias
                                .getFunction()));
                    }
                }
            }
            aliasListString = betweenBraces(join(listAliasString, COMMA + "\n"));
        }
    }

    private String formatAggregate() throws OperationNotSupportedException {
        List<Pipeline> pipelineList = new ArrayList<>();
        String conditionBuilderString = getConditionBuilderString();
        if (!conditionBuilderString.equals("{}")) {
            pipelineList.add(MongoMatch.create(conditionBuilderString));
        }
        if (this.pipelineList != null && !this.pipelineList.isEmpty()) {
            pipelineList = new ArrayList<>();
            if (!conditionBuilderString.equals("{}")) {
                pipelineList.add(MongoMatch.create(conditionBuilderString));
            }
            pipelineList.addAll(this.pipelineList);
        }
        return betweenBrackets(StringUtils.join(pipelineList, "\n,"));
    }

    public String getAggregateString() {
        return aggregateString;
    }

    public int getLimit() {
        return limit;
    }

    public String getSortString() {
        return sortString;
    }

    public String getAliasListString() {
        return aliasListString;
    }

    public String getProjectionString() {
        return projectionString;
    }

    public String getFilterString() {
        return filterString;
    }
}
