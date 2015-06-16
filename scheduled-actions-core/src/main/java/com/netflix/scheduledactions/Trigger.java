package com.netflix.scheduledactions;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import rx.functions.Action1;

/**
 * @author sthadeshwar
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface Trigger {
    public com.netflix.fenzo.triggers.Trigger<Context> createFenzoTrigger(Context context, Class<? extends Action1<Context>> action);
}
