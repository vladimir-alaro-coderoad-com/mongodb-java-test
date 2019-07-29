package com.mongodb.services;

import com.mongodb.dao.MongoDAO;
import com.mongodb.util.Constants;
import com.mongodb.util.Pagination;
import com.mongodb.util.ParseUtils;
import com.mongodb.util.PropertiesService;
import com.tierconnect.riot.api.database.base.DataBase;
import com.tierconnect.riot.api.database.base.GenericOperator;
import com.tierconnect.riot.api.database.base.Operation;
import com.tierconnect.riot.api.database.base.conditions.ConditionBuilder;
import com.tierconnect.riot.api.database.base.operator.SubQueryOperatorAggregate;
import com.tierconnect.riot.api.database.base.ordination.Order;
import com.tierconnect.riot.api.database.mongo.Mongo;
import com.tierconnect.riot.api.database.mongo.aggregate.MongoGroupBy;
import com.tierconnect.riot.api.database.mongo.aggregate.MongoLimit;
import com.tierconnect.riot.api.database.mongo.aggregate.MongoSkip;
import com.tierconnect.riot.api.database.mongo.aggregate.MongoSort;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import org.bson.Document;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Dependent
public class MongoShellServices {

    @Inject
    MongoDAO mongoDAO;

    @Inject
    Logger logger;

    @Inject
    transient PropertiesService propertiesService;

    private static final int MAXIMUN_ITEMS = 432426;


    public List<Map<String, Object>> getDocsWithCommandFindWithSubQueryOP1(JsonObject payload, Pagination pagination) throws Exception {
        Document mainFilter = ParseUtils.parseJsonToDocument(payload);
        String dataBaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);
        ConditionBuilder builder = new ConditionBuilder();

        //filter
        mainFilter.forEach((k, v) -> builder.addOperator(Operation.equals(k, v)));

        //subquery
//        int totalSnapshots = verifyInQueryExceedsBufferLimit((SubQueryOperatorAggregate) getSubQuery(0, 0));
        int totalSnapshots = 0;
        if (totalSnapshots > MAXIMUN_ITEMS) {
            int numberOfPage = (totalSnapshots / MAXIMUN_ITEMS);
            if ((totalSnapshots % MAXIMUN_ITEMS) != 0) {
                numberOfPage++;
            }
            List<GenericOperator> operators = new ArrayList<>();
            int tempSkip = 0;
            for (int i = 1; i <= numberOfPage; i++) {
                operators.add(getSubQuery(tempSkip, MAXIMUN_ITEMS, Collections.emptyList()));
                tempSkip = tempSkip + MAXIMUN_ITEMS;
            }
            builder.addOperator(Operation.OR(operators));
        } else {
            builder.addOperator(getSubQuery(0, 0, Collections.emptyList()));
        }

        Mongo mongoShell = new Mongo(mongoDAO.shellConnection(dataBaseName), builder);
        mongoShell.executeFind(Constants.HISTORY_COLLECTION, null, pagination.getSkip(), pagination.getLimit(),
                Collections.singletonMap("time", Order.DESC), null, "", null,
                DataBase.ExecutionResultScope.INCLUDE_RESULT);
        return mongoShell.getResultSet();
    }

    public List<Map<String, Object>> getDocsWithCommandFindWithSubQueryOP2(JsonObject payload, Pagination pagination) throws Exception {
        Document mainFilter = ParseUtils.parseJsonToDocument(payload);
        String dataBaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);
        ConditionBuilder builder = new ConditionBuilder();

        //filter
        mainFilter.forEach((k, v) -> builder.addOperator(Operation.equals(k, v)));


        Mongo mongoShell = new Mongo(mongoDAO.shellConnection(dataBaseName), builder);

        List<Pipeline> pipelines = getPipelinesForThingSnapshots(pagination.getSkip(), pagination.getLimit());

        mongoShell.executeAggregate(Constants.HISTORY_COLLECTION, pipelines, Boolean.FALSE);
        return mongoShell.getResultSet();
    }

    private GenericOperator getSubQuery(int skip, int limit, List<GenericOperator> operators) {
        return Operation.inSubqueryAggregate(Constants._ID, new ConditionBuilder().addAllOperator(operators),
                Constants.INDEX_COLLECTION, Constants.OID, getPipelinesForThingSnapshotsId(skip, limit));
    }

    private List<Pipeline> getPipelinesForThingSnapshotsId(int skip, int limit) {
        MongoSort mongoSort = new MongoSort();
        List<Pipeline> pipelines = new ArrayList<>();

        mongoSort.addSort(Constants.TIME, Order.DESC);
        MongoGroupBy group = new MongoGroupBy(ParseUtils._$(Constants.THING_ID));

        group.addAccumulator(Constants.OID, MongoGroupBy.Accumulator.FIRST, ParseUtils._$(Constants._ID));

        pipelines.add(mongoSort);
        pipelines.add(group);
        if (skip > 0) {
            pipelines.add(MongoSkip.create(skip));
        }
        if (limit > 0) {
            pipelines.add(MongoLimit.create(limit));
        }

        return pipelines;
    }

    private List<Pipeline> getPipelinesForThingSnapshots(int skip, int limit) {
        MongoSort mongoSort = new MongoSort();
        List<Pipeline> pipelines = new ArrayList<>();

        mongoSort.addSort("time", Order.DESC);
        MongoGroupBy group = new MongoGroupBy(ParseUtils._$(Constants._ID));

        group.addAccumulator(Constants.OID, MongoGroupBy.Accumulator.FIRST, ParseUtils._$(Constants.VALUE));

        pipelines.add(mongoSort);
        pipelines.add(group);
        if (skip > 0) {
            pipelines.add(MongoSkip.create(skip));
        }
        if (limit > 0) {
            pipelines.add(MongoLimit.create(limit));
        }

        return pipelines;
    }

    int verifyInQueryExceedsBufferLimit(SubQueryOperatorAggregate inQueryOperator) {
        int exceedsBufferLimit = MAXIMUN_ITEMS;
        String dataBaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);

        Mongo mongo = new Mongo(mongoDAO.shellConnection(dataBaseName), inQueryOperator.getCondition());

        List<Pipeline> pipelines = new ArrayList<>(inQueryOperator.getPipelines());
        try {
            // add count
            MongoGroupBy mongoGroupCount = new MongoGroupBy();
            mongoGroupCount.setSingleAccumulator("COUNT", MongoGroupBy.Accumulator.SUM, 1);
            pipelines.add(mongoGroupCount);
            mongo.executeAggregate(Constants.INDEX_COLLECTION, pipelines);
            List<Map<String, Object>> resultSet = mongo.getResultSet();
            logger.info("Size for operator 'IN ' : " + resultSet);
            if (resultSet != null && !resultSet.isEmpty()) {
                Map<String, Object> mapCount = resultSet.get(0);
                Number count = (Number) mapCount.get("COUNT");
                if (count.longValue() > MAXIMUN_ITEMS) {
                    logger.warning("The quantity of the documents in thingSnapshotIndex exceeds the buffer limit ");
                    exceedsBufferLimit = count.intValue();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exceedsBufferLimit;
    }
}
