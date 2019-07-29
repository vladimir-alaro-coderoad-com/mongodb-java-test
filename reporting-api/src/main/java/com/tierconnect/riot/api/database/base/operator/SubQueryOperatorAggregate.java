package com.tierconnect.riot.api.database.base.operator;

import com.tierconnect.riot.api.database.base.GenericOperator;
import com.tierconnect.riot.api.database.base.Operation;
import com.tierconnect.riot.api.database.base.conditions.ConditionBuilder;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;

import java.util.List;

import static com.tierconnect.riot.api.assertions.Assertions.voidNotNull;
import static com.tierconnect.riot.api.mongoShell.utils.CharacterUtils.betweenDoubleQuote;

public class SubQueryOperatorAggregate implements GenericOperator {

    private final String key;
    private final ConditionBuilder condition;
    private final String target;
    private final String fieldToProject;
    private final List<Pipeline> pipelines;
    private final Operation.OperationEnum operator;

    public SubQueryOperatorAggregate(String key, ConditionBuilder condition, String target, String fieldToProject, Operation.OperationEnum operator) {
        this(key, condition, target, fieldToProject, operator, null);
    }

    public SubQueryOperatorAggregate(String key, ConditionBuilder condition, String target, String fieldToProject, Operation.OperationEnum operator, List<Pipeline> pipelines) {
        voidNotNull("key", key);
        voidNotNull("condition", condition);
        voidNotNull("target", target);
        voidNotNull("fieldToProject", fieldToProject);
        voidNotNull("operator", operator);
        this.key = key;
        this.condition = condition;
        this.target = target;
        this.fieldToProject = fieldToProject;
        this.operator = operator;
        this.pipelines = pipelines;
    }

    public String getKey() {
        return key;
    }

    public String getKeyWithQuote() {
        return betweenDoubleQuote(key);
    }

    @Override
    public boolean isMultipleOperator() {
        return false;
    }

    public ConditionBuilder getCondition() {
        return condition;
    }

    public String   getTarget() {
        return target;
    }

    public String getFieldToProject() {
        return fieldToProject;
    }

    public Operation.OperationEnum getOperator() {
        return operator;
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
    }
}
