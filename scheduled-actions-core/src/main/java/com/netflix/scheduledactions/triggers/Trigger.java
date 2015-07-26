package com.netflix.scheduledactions.triggers;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.scheduledactions.Context;
import rx.functions.Action1;

/**
 * @author sthadeshwar
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface Trigger {
    public void validate() throws IllegalArgumentException;
    public com.netflix.fenzo.triggers.Trigger<Context> createFenzoTrigger(Context context,
                                                                          Class<? extends Action1<Context>> action);
}
