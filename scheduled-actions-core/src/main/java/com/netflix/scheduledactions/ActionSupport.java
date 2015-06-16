package com.netflix.scheduledactions;

import java.util.List;

/**
 * @author sthadeshwar
 */
public abstract class ActionSupport implements Action {

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public List<String> getOwners() {
        return null;
    }

    @Override
    public List<String> getWatchers() {
        return null;
    }

    @Override
    public List<String> getExecutionLog() {
        return null;
    }

}
