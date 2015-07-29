package com.netflix.scheduledactions;

import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException;
import com.netflix.scheduledactions.exceptions.ExecutionNotFoundException;
import com.netflix.scheduledactions.executors.LocalThreadPoolBlockingExecutor;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import com.netflix.scheduledactions.persistence.ExecutionDao;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Primary class that operates on {@code ActionInstance}. This class should be initialized
 * as a singleton in the application that is using this library.
 * @author sthadeshwar
 */
public class ActionsOperator {

    public static final String DEFAULT_INITIATOR = "Unknown";

    private final ActionInstanceDao actionInstanceDao;
    private final ExecutionDao executionDao;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ActionOperationsDelegate actionOperationsDelegate;

    /**
     * Creates a new instance of {@code ActionsOperator} with a {@code LocalActionOperationsDelegate}
     * @param triggerOperator
     * @param daoConfigurer
     * @param threadPoolSize
     */
    public ActionsOperator(TriggerOperator triggerOperator, DaoConfigurer daoConfigurer, int threadPoolSize) {
        this(UUID.randomUUID().toString(), triggerOperator, daoConfigurer, threadPoolSize);
    }

    /**
     * Creates a new instance of {@code ActionsOperator} with a {@code LocalActionOperationsDelegate}
     * @param id
     * @param triggerOperator
     * @param daoConfigurer
     * @param threadPoolSize
     */
    public ActionsOperator(String id,
                           TriggerOperator triggerOperator,
                           DaoConfigurer daoConfigurer,
                           int threadPoolSize) {
        this(
            daoConfigurer,
            new DefaultActionOperationsDelegate(
                id,
                triggerOperator,
                daoConfigurer,
                new LocalThreadPoolBlockingExecutor(daoConfigurer.getExecutionDao(), threadPoolSize),
                threadPoolSize
            )
        );
    }

    /**
     * Creates a new instance of {@code ActionsOperator} with a either a {@code LocalActionOperationsDelegate} or
     * {@code LocalActionOperationsDelegate} depending on the {@code clustered} flag. If the cluster flag is passed
     * 'true' then an implementation of {@code LocalActionOperationsDelegate} has to be passed along
     * @param daoConfigurer
     * @param actionOperationsDelegate
     */
    public ActionsOperator(DaoConfigurer daoConfigurer,
                           ActionOperationsDelegate actionOperationsDelegate) {
        this.actionInstanceDao = daoConfigurer.getActionInstanceDao();
        this.executionDao = daoConfigurer.getExecutionDao();
        this.actionOperationsDelegate = actionOperationsDelegate;
        validate();
    }

    private void validate() {
        if (actionOperationsDelegate == null) {
            throw new IllegalArgumentException("ActionOperationsDelegate cannot be null");
        }
    }

    @PostConstruct
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            this.actionOperationsDelegate.initialize();
        }
    }

    @PreDestroy
    public void destroy() throws SchedulerException {
        if (initialized.get()) {
            this.actionOperationsDelegate.destroy();
        }
    }

    /**
     * Returns the {@code ActionInstance} based on the unique actionInstance id
     * @param actionInstanceId
     * @return
     */
    public ActionInstance getActionInstance(String actionInstanceId) {
        return actionInstanceDao.getActionInstance(actionInstanceId);
    }

    /**
     * Returns a list of {@code ActionInstance}s registered with the {@code ActionsOperator} for the given actionInstanceGroup
     * @param group
     * @return
     */
    public List<ActionInstance> getActionInstances(String group) {
        List<ActionInstance> actionInstances = actionInstanceDao.getActionInstances(group);
        if (actionInstances != null) {
            Collections.sort(actionInstances);
        }
        return actionInstances;
    }

    /**
     * Returns a list of all the {@code ActionInstance}s registered with the {@code ActionsOperator}
     * @return
     */
    public List<ActionInstance> getActionInstances() {
        List<ActionInstance> actionInstances = actionInstanceDao.getActionInstances();
        if (actionInstances != null) {
            Collections.sort(actionInstances);
        }
        return actionInstances;
    }

    /**
     * Returns a list of {@code Execution}s for a given actionInstance
     * @param actionInstanceId
     * @return
     */
    public List<Execution> getExecutions(String actionInstanceId, int count) {
        return executionDao.getExecutions(actionInstanceId, count);
    }

    /**
     * Returns a list of {@code Execution}s for a given actionInstance
     * @param actionInstanceId
     * @return
     */
    public List<Execution> getExecutions(String actionInstanceId) {
        List<Execution> executions = executionDao.getExecutions(actionInstanceId);
        if (executions != null) {
            Collections.sort(executions);
        }
        return executions;
    }

    /**
     *
     * @param executionId
     * @return
     */
    public Execution getExecution(String executionId) {
        return executionDao.getExecution(executionId);
    }

    /**
     * Registers a {@code ActionInstance} with actionInstance service
     * @param actionInstance
     */
    public String registerActionInstance(ActionInstance actionInstance) {
        checkInitialized();
        return actionOperationsDelegate.register(actionInstance);
    }

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance disableActionInstance(String actionInstanceId) throws ActionInstanceNotFoundException {
        checkInitialized();
        return actionOperationsDelegate.disable(actionInstanceId);
    }

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstance
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void disableActionInstance(ActionInstance actionInstance) {
        checkInitialized();
        actionOperationsDelegate.disable(actionInstance);
    }

    /**
     * Enables the {@code ActionInstance} associated with this actionInstanceId
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance enableActionInstance(String actionInstanceId) throws ActionInstanceNotFoundException {
        checkInitialized();
        return actionOperationsDelegate.enable(actionInstanceId);
    }

    /**
     * Enables the {@code ActionInstance}
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void enableActionInstance(ActionInstance actionInstance) {
        checkInitialized();
        actionOperationsDelegate.enable(actionInstance);
    }

    /**
     * Updates the existing {@code ActionInstance}
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void updateActionInstance(ActionInstance actionInstance) {
        checkInitialized();
        actionOperationsDelegate.update(actionInstance);
    }

    /**
     * Deletes/Removes the {@code ActionInstance} associated with this actionInstanceId.
     * If it has a {@code CronTrigger} then it is also un-scheduled from scheduler
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance deleteActionInstance(String actionInstanceId) throws ActionInstanceNotFoundException {
        checkInitialized();
        return actionOperationsDelegate.delete(actionInstanceId);
    }

    /**
     * Deletes/Removes the {@code ActionInstance}. If it has a {@code CronTrigger} then it is also un-scheduled from
     * scheduler
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void deleteActionInstance(ActionInstance actionInstance) {
        checkInitialized();
        actionOperationsDelegate.delete(actionInstance);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @Deprecated
     */
    public Execution execute(String actionInstanceId) throws ActionInstanceNotFoundException {
        checkInitialized();
        return actionOperationsDelegate.execute(actionInstanceId, DEFAULT_INITIATOR);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public Execution execute(String actionInstanceId, String initiator) throws ActionInstanceNotFoundException {
        checkInitialized();
        return actionOperationsDelegate.execute(actionInstanceId, initiator);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstance
     * @Deprecated
     */
    public Execution execute(ActionInstance actionInstance) {
        checkInitialized();
        return actionOperationsDelegate.execute(actionInstance, DEFAULT_INITIATOR);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstance
     * @param initiator
     * @throws com.netflix.scheduledactions.exceptions.ExecutionException
     */
    public Execution execute(ActionInstance actionInstance, String initiator)  {
        checkInitialized();
        return actionOperationsDelegate.execute(actionInstance, initiator);
    }

    /**
     * Cancels the currently running {@code Execution} for given {@code Execution} id
     * @param executionId
     * @throws ActionInstanceNotFoundException
     */
    public void cancel(String executionId) throws ExecutionNotFoundException, ActionInstanceNotFoundException {
        checkInitialized();
        actionOperationsDelegate.cancel(executionId);
    }

    /**
     * Cancels the {@code Execution}
     * @param actionInstance
     */
    public void cancel(Execution execution, ActionInstance actionInstance) {
        checkInitialized();
        actionOperationsDelegate.cancel(execution, actionInstance);
    }

    /**
     * Checks to see if this instance is initialized or not
     */
    private void checkInitialized() {
        if (!initialized.get()) throw new RuntimeException("ActionsOperator instance is not initialized. " +
            "Clients not using dependency injection need to explicitly call initialize() method on ActionsOperator instance");
    }
}
