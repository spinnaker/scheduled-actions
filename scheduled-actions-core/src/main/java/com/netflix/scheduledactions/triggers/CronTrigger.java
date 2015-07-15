package com.netflix.scheduledactions.triggers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.scheduledactions.Context;
import rx.functions.Action1;

/**
 * @author sthadeshwar
 */
public class CronTrigger implements Trigger {

    private final String cronExpression;

    @JsonCreator
    public CronTrigger(@JsonProperty("cronExpression") String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public com.netflix.fenzo.triggers.CronTrigger<Context> createFenzoTrigger(Context context, Class<? extends Action1<Context>> action) {
        return new com.netflix.fenzo.triggers.CronTrigger<>(this.cronExpression, context.getName(), context, Context.class, action);
    }

    public String getCronExpression() {
        return cronExpression;
    }

    @Override
    public String toString() {
        return "CronTrigger (" + cronExpression + ')';
    }
}
