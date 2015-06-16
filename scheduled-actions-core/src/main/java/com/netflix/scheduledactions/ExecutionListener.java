package com.netflix.scheduledactions;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author sthadeshwar
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface ExecutionListener {

    /**
     * Gets called when an execution for an actionInstance starts
     * @param context
     * @param execution
     */
    public void onStart(Context context, Execution execution);

    /**
     ** Gets called after an execution for an actionInstance ends
     * @param context
     * @param execution
     */
    public void onComplete(Context context, Execution execution);

    /**
     * Gets called when an execution results in a {@code Status.FAILURE}
     * @param context
     * @param execution
     */
    public void onError(Context context, Execution execution);

    /**
     * Gets called when an execution is cancelled
     * @param context
     * @param execution
     */
    public void beforeCancel(Context context, Execution execution);

}
