package com.tierconnect.riot.api.database.mongo.aggregate;

import com.mongodb.client.model.BsonField;
import com.tierconnect.riot.api.assertions.Assertions;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import com.tierconnect.riot.api.database.mongo.pipeline.PipelineBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.group;
import static com.tierconnect.riot.api.mongoShell.utils.CharacterUtils.COLON;
import static com.tierconnect.riot.api.mongoShell.utils.CharacterUtils.betweenBraces;
import static java.util.Collections.singletonMap;

/**
 * Created by vealaro on 1/30/17.
 */
public class MongoGroupBy extends PipelineBase implements Pipeline {

    private static Logger logger = LogManager.getLogger(MongoGroupBy.class);
    private Map<String, Object> groupID;
    private String groupIDString;
    private List<AccumulatorOperator> accumulators = new ArrayList<>();

    public MongoGroupBy() {
        groupID = null;
        groupIDString = null;
    }

    public MongoGroupBy(String group) {
        this.groupIDString = group;
    }

    public MongoGroupBy(Map<String, Object> group) {
        this.groupID = group;
    }

    public MongoGroupBy(Map<String, Object> groupID, String labelGroupBy, Accumulator accumulator, Object valueGroupBy) {
        Assertions.notNull("Label Group By", labelGroupBy);
        Assertions.notNull("Accumulator operator", accumulator);
        Assertions.notNull("Value Group By", valueGroupBy);
        this.groupID = groupID;
        setSingleAccumulator(labelGroupBy, accumulator, valueGroupBy);
    }

    public MongoGroupBy(String groupID, String labelGroupBy, Accumulator accumulator, Object valueGroupBy) {
        Assertions.notNull("Label Group By", labelGroupBy);
        Assertions.notNull("Accumulator operator", accumulator);
        Assertions.notNull("Value Group By", valueGroupBy);
        this.groupIDString = groupID;
        setSingleAccumulator(labelGroupBy, accumulator, valueGroupBy);
    }

    public void addAccumulator(String labelGroupBy, Accumulator accumulator, Object valueGroupBy) {
        Assertions.notNull("Label Group By", labelGroupBy);
        Assertions.notNull("Accumulator operator", accumulator);
        Assertions.notNull("Value Group By", valueGroupBy);
        addSingleAccumulator(labelGroupBy, accumulator, valueGroupBy);
    }

    public void setGroupID(Map<String, Object> groupID) {
        this.groupID = groupID;
    }

    public Object getGroupID() {
        if (groupIDString != null) {
            return groupIDString;
        } else if (groupID != null) {
            return new Document(groupID);
        } else {
            return null;
        }
    }

    public void setSingleAccumulator(String labelGroupBy, Accumulator accumulator, Object valueGroupBy) {
        accumulators.clear();
        accumulators.add(new AccumulatorOperator(labelGroupBy, accumulator, valueGroupBy));
    }

    private void addSingleAccumulator(String labelGroupBy, Accumulator accumulator, Object valueGroupBy) {
        accumulators.add(new AccumulatorOperator(labelGroupBy, accumulator, valueGroupBy));
    }

    public Bson toBson() {
        Bson groupbson;
        if (accumulators.isEmpty()) {
            groupbson = group(getGroupID());
        } else {
            groupbson = group(getGroupID(), toMultipleBsonField());
        }
        return groupbson;
    }

    private List<BsonField> toMultipleBsonField() {
        return accumulators.stream().map(this::toSingleBsonField).collect(Collectors.toList());
    }

    private BsonField toSingleBsonField(AccumulatorOperator accumulatorOperator) {
        if (Accumulator.FIRST.equals(accumulatorOperator.accumulator)) {
            return first(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        } else if (Accumulator.LAST.equals(accumulatorOperator.accumulator)) {
            return last(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        } else if (Accumulator.AVG.equals(accumulatorOperator.accumulator)) {
            return avg(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        } else if (Accumulator.SUM.equals(accumulatorOperator.accumulator)) {
            return sum(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        } else if (Accumulator.MIN.equals(accumulatorOperator.accumulator)) {
            return min(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        } else if (Accumulator.MAX.equals(accumulatorOperator.accumulator)) {
            return max(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        } else if (Accumulator.COUNT.equals(accumulatorOperator.accumulator)
                && accumulatorOperator.valueGroupBy instanceof Map) {
            return sum(accumulatorOperator.labelGroupBy, accumulatorOperator.valueGroupBy);
        }
        return sum(accumulatorOperator.labelGroupBy, 1);
    }

    @Override
    public String toString() {
        StringBuilder group = new StringBuilder(GROUP).append(COLON);
        Map<String, Object> mapGroup = new LinkedHashMap<>();
        mapGroup.put("_id", getGroupID());
        if (accumulators != null && !accumulators.isEmpty()) {
            for (AccumulatorOperator accumulatorOperator : accumulators) {
                if (accumulatorOperator.valueGroupBy instanceof Map) {
                    if (((Map) accumulatorOperator.valueGroupBy).containsKey("$cond")) {
                        mapGroup.put(
                                accumulatorOperator.labelGroupBy,
                                singletonMap(accumulatorOperator.accumulator.getValue(), accumulatorOperator.valueGroupBy)
                        );
                    } else {
                        mapGroup.put(
                                accumulatorOperator.labelGroupBy,
                                accumulatorOperator.valueGroupBy
                        );
                    }
                } else {
                    mapGroup.put(
                            accumulatorOperator.labelGroupBy,
                            singletonMap(accumulatorOperator.accumulator.getValue(), accumulatorOperator.valueGroupBy)
                    );
                }
            }
        }
        group.append(mapToJson(mapGroup));
        return betweenBraces(group.toString());
    }

    public String mapToJson(Map map) {
        try {
            return super.mapToJson(map);
        } catch (IOException e) {
            logger.error("Error convert map to json string in Mongo GroupBy : \n " + map, e);
        }
        return null;
    }

    public enum Accumulator {

        SUM("$sum"),
        AVG("$avg"),
        MAX("$max"),
        MIN("$min"),
        COUNT("$sum"),
        FIRST("$first"),
        LAST("$last");

        private String value;

        Accumulator(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Accumulator getObject(String value) {
            return Accumulator.valueOf(value);
        }
    }

    private class AccumulatorOperator {
        private String labelGroupBy;
        private Accumulator accumulator;
        private Object valueGroupBy;

        private AccumulatorOperator(String labelGroupBy, Accumulator accumulator, Object valueGroupBy) {
            if (Accumulator.COUNT.equals(accumulator)) {
                if (!(valueGroupBy instanceof Map)) {
                    // set default value
                    valueGroupBy = 1;
                }
            }
            this.labelGroupBy = labelGroupBy;
            this.accumulator = accumulator;
            this.valueGroupBy = valueGroupBy;
        }
    }
}
