package com.netflix.scheduledactions;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author sthadeshwar
 */
public class ActionInstance {

    public static enum ConcurrentExecutionStrategy {
        ALLOW("ALLOW"),
        REJECT("REJECT"),
        REPLACE("REPLACE");

        private final String strategy;
        ConcurrentExecutionStrategy(String strategy) { this.strategy = strategy; }
        public String getStrategy() { return strategy; }
    }

    private String id;
    private String name;
    private String group;
    private Class<? extends Action> action;
    private Class<? extends ExecutionListener> executionListener;
    private Map<String, String> parameters;
    private Trigger trigger;
    private com.netflix.fenzo.triggers.Trigger<Context> fenzoTrigger;
    private List<String> owners;
    private List<String> watchers;
    private boolean disabled;
    private Date lastUpdated;
    private long executionTimeoutInSeconds;
    private ConcurrentExecutionStrategy concurrentExecutionStrategy;
    private Context context;

    private ActionInstance() {}

    private ActionInstance(Builder builder) {
        this.name = builder.name;
        this.group = builder.group;
        this.action = builder.action;
        this.parameters = builder.parameters;
        this.trigger = builder.trigger;
        this.owners = builder.owners;
        this.watchers = builder.watchers;
        this.executionListener = builder.executionListener;
        this.executionTimeoutInSeconds = builder.executionTimeoutInSeconds;
        this.concurrentExecutionStrategy = builder.concurrentExecutionStrategy;
        this.lastUpdated = new Date();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Class<? extends Action> getAction() {
        return action;
    }

    public void setAction(Class<? extends Action> action) {
        this.action = action;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }

    public List<String> getWatchers() {
        return watchers;
    }

    public void setWatchers(List<String> watchers) {
        this.watchers = watchers;
    }

    public Class<? extends ExecutionListener> getExecutionListener() {
        return executionListener;
    }

    public void setExecutionListener(Class<? extends ExecutionListener> executionListener) {
        this.executionListener = executionListener;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public long getExecutionTimeoutInSeconds() {
        return executionTimeoutInSeconds;
    }

    public void setExecutionTimeoutInSeconds(long executionTimeoutInSeconds) {
        this.executionTimeoutInSeconds = executionTimeoutInSeconds;
    }

    public ConcurrentExecutionStrategy getConcurrentExecutionStrategy() {
        return concurrentExecutionStrategy;
    }

    public void setConcurrentExecutionStrategy(ConcurrentExecutionStrategy concurrentExecutionStrategy) {
        this.concurrentExecutionStrategy = concurrentExecutionStrategy;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public Context getContext() {
        return context;
    }

    public com.netflix.fenzo.triggers.Trigger<Context> getFenzoTrigger() {
        return fenzoTrigger;
    }

    public void setFenzoTrigger(com.netflix.fenzo.triggers.Trigger<Context> fenzoTrigger) {
        this.fenzoTrigger = fenzoTrigger;
    }

    public static Context createContext(ActionInstance actionInstance) {
        return new Context(actionInstance.id, actionInstance.name, actionInstance.group, actionInstance.parameters);
    }

    public static Builder newActionInstance() {
        return new Builder();
    }

    @Override
    public String toString() {
        return id != null ? id : name;
    }

    public static class Builder {
        private String name;
        private String group = "DEFAULT_GROUP";
        private Class<? extends Action> action;
        private Map<String, String> parameters;
        private Trigger trigger;
        private List<String> owners;
        private List<String> watchers;
        private Class<? extends ExecutionListener> executionListener = NoOpExecutionListener.class;
        private long executionTimeoutInSeconds = -1L;
        private ConcurrentExecutionStrategy concurrentExecutionStrategy = ConcurrentExecutionStrategy.REJECT;

        private Builder() {}

        Builder withName(String name) {
            this.name = name;
            return this;
        }

        Builder withGroup(String group) {
            this.group = group;
            return this;
        }

        Builder withAction(Class<? extends Action> action) {
            this.action = action;
            return this;
        }

        Builder withParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        Builder withTrigger(Trigger trigger) {
            this.trigger = trigger;
            return this;
        }

        Builder withOwners(String... owners) {
            this.owners = Arrays.asList(owners);
            return this;
        }

        Builder withWatchers(String... watchers) {
            this.watchers = Arrays.asList(watchers);
            return this;
        }

        Builder withExecutionListener(Class<? extends ExecutionListener> executionListener) {
            this.executionListener = executionListener;
            return this;
        }

        Builder withExecutionTimeoutInSeconds(long executionTimeoutInSeconds) {
            this.executionTimeoutInSeconds = executionTimeoutInSeconds;
            return this;
        }

        Builder withConcurrentExecutionStrategy(ConcurrentExecutionStrategy concurrentExecutionStrategy) {
            this.concurrentExecutionStrategy = concurrentExecutionStrategy;
            return this;
        }

        ActionInstance build() {
            return new ActionInstance(this);
        }
    }

    public static class NoOpExecutionListener implements ExecutionListener {
        @Override
        public void onStart(Context context, Execution execution) {}

        @Override
        public void onComplete(Context context, Execution execution) {}

        @Override
        public void onError(Context context, Execution execution) {}

        @Override
        public void beforeCancel(Context context, Execution execution) {}
    }
}
