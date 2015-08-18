package com.netflix.scheduledactions;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.netflix.scheduledactions.triggers.Trigger;

import java.util.*;

/**
 * @author sthadeshwar
 */
@JsonDeserialize(builder = ActionInstance.ActionInstanceBuilder.class)
public class ActionInstance implements Comparable<ActionInstance> {

    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";
    public static final Class<? extends ExecutionListener> DEFAULT_EXECUTION_LISTENER_CLASS = NoOpExecutionListener.class;
    public static final long DEFAULT_EXECUTION_TIMEOUT = -1L;
    public static final ConcurrentExecutionStrategy DEFAULT_EXECUTION_STRATEGY = ConcurrentExecutionStrategy.REJECT;

    private long creationTime;
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

    private ActionInstance(ActionInstanceBuilder builder) {
        this.creationTime = builder.creationTime;
        this.id = builder.id;
        this.name = builder.name;
        this.group = builder.group;
        this.action = builder.action;
        this.executionListener = builder.executionListener;
        this.parameters = builder.parameters;
        this.trigger = builder.trigger;
        this.fenzoTrigger = builder.fenzoTrigger;
        this.owners = builder.owners;
        this.watchers = builder.watchers;
        this.disabled = builder.disabled;
        this.lastUpdated = builder.lastUpdated;
        this.executionTimeoutInSeconds = builder.executionTimeoutInSeconds;
        this.concurrentExecutionStrategy = builder.concurrentExecutionStrategy;
        this.context = builder.context;
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

    void setAction(Class<? extends Action> action) {
        this.action = action;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    void setParameters(Map<String, String> parameters) {
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

    void setOwners(List<String> owners) {
        this.owners = owners;
    }

    public List<String> getWatchers() {
        return watchers;
    }

    void setWatchers(List<String> watchers) {
        this.watchers = watchers;
    }

    public Class<? extends ExecutionListener> getExecutionListener() {
        return executionListener;
    }

    void setExecutionListener(Class<? extends ExecutionListener> executionListener) {
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

    void setExecutionTimeoutInSeconds(long executionTimeoutInSeconds) {
        this.executionTimeoutInSeconds = executionTimeoutInSeconds;
    }

    public ConcurrentExecutionStrategy getConcurrentExecutionStrategy() {
        return concurrentExecutionStrategy;
    }

    void setConcurrentExecutionStrategy(ConcurrentExecutionStrategy concurrentExecutionStrategy) {
        this.concurrentExecutionStrategy = concurrentExecutionStrategy;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public Context getContext() {
        context.setActionInstanceId(this.id);
        return context;
    }

    public com.netflix.fenzo.triggers.Trigger<Context> getFenzoTrigger() {
        return fenzoTrigger;
    }

    void setFenzoTrigger(com.netflix.fenzo.triggers.Trigger<Context> fenzoTrigger) {
        this.fenzoTrigger = fenzoTrigger;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public int compareTo(ActionInstance o) {
        if (o != null) {
            return this.creationTime - o.creationTime == 0 ?  0 :
                   this.creationTime - o.creationTime  < 0 ? -1 : 1;
        }
        return 0;
    }

    public static ActionInstanceBuilder newActionInstance() {
        return new ActionInstanceBuilder();
    }

    @Override
    public String toString() {
        return id != null ? id : name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActionInstance that = (ActionInstance) o;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    public static class ActionInstanceBuilder {
        private long creationTime;
        private String id;
        private String name;
        private String group = DEFAULT_GROUP;
        private Class<? extends Action> action;
        private Class<? extends ExecutionListener> executionListener = DEFAULT_EXECUTION_LISTENER_CLASS;
        private Map<String, String> parameters;
        private Trigger trigger;
        private com.netflix.fenzo.triggers.Trigger<Context> fenzoTrigger;
        private List<String> owners;
        private List<String> watchers;
        private boolean disabled;
        private Date lastUpdated = new Date();
        private long executionTimeoutInSeconds = DEFAULT_EXECUTION_TIMEOUT;
        private ConcurrentExecutionStrategy concurrentExecutionStrategy = DEFAULT_EXECUTION_STRATEGY;
        private Context context;

        private ActionInstanceBuilder() {}

        public ActionInstanceBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public ActionInstanceBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public ActionInstanceBuilder withGroup(String group) {
            this.group = group;
            return this;
        }

        public ActionInstanceBuilder withAction(Class<? extends Action> action) {
            this.action = action;
            return this;
        }

        public ActionInstanceBuilder withParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        public ActionInstanceBuilder withTrigger(Trigger trigger) {
            this.trigger = trigger;
            return this;
        }

        public ActionInstanceBuilder withOwners(String... owners) {
            this.owners = owners != null ? Arrays.asList(owners) : Collections.<String>emptyList();
            return this;
        }

        public ActionInstanceBuilder withWatchers(String... watchers) {
            this.watchers = watchers != null ? Arrays.asList(watchers) : Collections.<String>emptyList();
            return this;
        }

        public ActionInstanceBuilder withExecutionListener(Class<? extends ExecutionListener> executionListener) {
            this.executionListener = executionListener;
            return this;
        }

        public ActionInstanceBuilder withExecutionTimeoutInSeconds(long executionTimeoutInSeconds) {
            this.executionTimeoutInSeconds = executionTimeoutInSeconds;
            return this;
        }

        public ActionInstanceBuilder withConcurrentExecutionStrategy(ConcurrentExecutionStrategy concurrentExecutionStrategy) {
            this.concurrentExecutionStrategy = concurrentExecutionStrategy;
            return this;
        }

        public ActionInstance build() {
            this.context = new Context(id, name, group, parameters);
            this.creationTime = System.currentTimeMillis();
            return new ActionInstance(this);
        }

        /**
         * Do NOT use. For internal (unit testing) use only
         * @return
         */
        public ActionInstance build(long creationTime) {
            this.context = new Context(id, name, group, parameters);
            this.creationTime = creationTime;
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
