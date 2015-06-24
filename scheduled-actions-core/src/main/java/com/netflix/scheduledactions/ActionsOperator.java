package com.netflix.scheduledactions;

import com.netflix.scheduledactions.exceptions.ActionOperationException;
import com.netflix.scheduledactions.exceptions.ExecutionException;
import com.netflix.scheduledactions.exceptions.ExecutionNotFoundException;
import com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException;
import com.netflix.scheduledactions.executors.Executor;
import com.netflix.scheduledactions.persistence.ExecutionDao;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * @author sthadeshwar
 */
public class ActionsOperator {

    private static final Logger logger = LoggerFactory.getLogger(ActionsOperator.class);

    public static final String DEFAULT_INITIATOR = "Unknown";

    private final TriggerOperator triggerOperator;
    private final ActionInstanceDao actionInstanceDao;
    private final ExecutionDao executionDao;
    private final Executor executor;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ExecutorService executeService;
    private final ExecutorService cancelService;

    private static ActionsOperator actionsOperator;

    private ActionsOperator(DaoConfigurer daoConfigurer, Executor executor, int threadPoolSize) {
        this.actionInstanceDao = daoConfigurer.getActionInstanceDao();
        this.triggerOperator = TriggerOperator.getInstance(daoConfigurer.getTriggerDao(), threadPoolSize);
        this.executor = executor;
        this.executionDao = executor.getExecutionDao();
        this.executeService = Executors.newFixedThreadPool(threadPoolSize);
        this.cancelService = Executors.newFixedThreadPool(threadPoolSize > 1 ? threadPoolSize/2 : threadPoolSize);
    }

    @PostConstruct
    public void initialize() throws SchedulerException {
        if (initialized.compareAndSet(false, true)) {
            this.triggerOperator.initialize();
        }
    }

    /**
     * Factory method to get a {@code ActionsOperator} instance
     * @param daoConfigurer
     * @param executor
     * @param threadPoolSize
     * @return
     */
    public static synchronized ActionsOperator getInstance(DaoConfigurer daoConfigurer, Executor executor, int threadPoolSize) {
        if (actionsOperator == null) {
            actionsOperator = new ActionsOperator(daoConfigurer, executor, threadPoolSize);
        }
        return actionsOperator;
    }

    /**
     *
     * @return
     */
    public static ActionsOperator getExistingOperator() {
        return actionsOperator;
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
     *
     * @param actionInstance
     * @return
     */
    public boolean isActionInstanceRegistered(ActionInstance actionInstance) {
        return actionInstance.getId() != null;
    }

    /**
     * Registers a {@code ActionInstance} with actionInstance service
     * @param actionInstance
     */
    public String registerActionInstance(ActionInstance actionInstance) {
        validateActionInstance(actionInstance);

        actionInstanceDao.createActionInstance(actionInstance.getGroup(), actionInstance);

        Context context = ActionInstance.createContext(actionInstance);
        actionInstance.setContext(context);

        if (actionInstance.getTrigger() != null) {
            actionInstance.setFenzoTrigger(actionInstance.getTrigger().createFenzoTrigger(context, InternalAction.class));
            try {
                triggerOperator.registerTrigger(actionInstance.getGroup(), actionInstance.getFenzoTrigger());
            } catch (SchedulerException e) {
                throw new ActionOperationException(String.format(
                    "Exception occurred while registering actionInstance %s", actionInstance), e);
            }
        }

        actionInstanceDao.updateActionInstance(actionInstance);

        logger.info("Successfully registered the actionInstance {}", actionInstance);
        return actionInstance.getId();
    }

    public static class InternalAction implements Action1<Context> {
        private static final Logger actionLogger = LoggerFactory.getLogger(InternalAction.class);

        @Override
        public void call(Context context) {
            try {
                ActionsOperator actionsOperator = ActionsOperator.getExistingOperator();
                actionsOperator.execute(context.getActionInstanceId());
            } catch (ActionInstanceNotFoundException e) {
                actionLogger.error("Exception occurred in InternalAction while calling actionsOperator.execute() for context {}", context, e);
            }
        }
    }

    /**
     *
     * @param actionInstance
     */
    private void validateActionInstance(ActionInstance actionInstance) {
        if (actionInstance == null) {
            throw new IllegalArgumentException("actionInstance cannot be null");
        }
        if (actionInstance.getName() == null || "".equals(actionInstance.getName())) {
            throw new IllegalArgumentException("name for the actionInstance cannot be null or empty");
        }
        if (actionInstance.getAction() == null) {
            throw new IllegalArgumentException("No Action class specified for the actionInstance");
        }
    }

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance disableActionInstance(String actionInstanceId) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            disableActionInstance(actionInstance);
        } else {
            throw new ActionInstanceNotFoundException("No actionInstance found with actionInstance id: " + actionInstanceId);
        }
        return actionInstance;
    }

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstance
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void disableActionInstance(ActionInstance actionInstance) {
        actionInstance.setDisabled(true);
        actionInstanceDao.updateActionInstance(actionInstance);
        if (actionInstance.getFenzoTrigger() != null) {
            try {
                triggerOperator.disableTrigger(actionInstance.getFenzoTrigger());
            } catch (SchedulerException e) {
                throw new ActionOperationException(String.format(
                    "Exception occurred while disabling trigger %s for actionInstance %s", actionInstance.getTrigger(), actionInstance), e);
            }
        }
        logger.info("Successfully disabled the actionInstance {}", actionInstance);
    }

    /**
     * Enables the {@code ActionInstance} associated with this actionInstanceId
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance enableActionInstance(String actionInstanceId) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            enableActionInstance(actionInstance);
        } else {
            throw new ActionInstanceNotFoundException("No actionInstance found with actionInstance id: " + actionInstanceId);
        }
        return actionInstance;
    }

    /**
     * Enables the {@code ActionInstance}
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void enableActionInstance(ActionInstance actionInstance) {
        actionInstance.setDisabled(false);
        actionInstanceDao.updateActionInstance(actionInstance);
        if (actionInstance.getFenzoTrigger() != null) {
            try {
                triggerOperator.enableTrigger(actionInstance.getFenzoTrigger());
            } catch (SchedulerException e) {
                throw new ActionOperationException(String.format(
                    "Exception occurred while enabling trigger %s for actionInstance %s", actionInstance.getTrigger(), actionInstance), e);
            }
        }
        logger.info("Successfully enabled the actionInstance {}", actionInstance);
    }

    /**
     * Deletes/Removes the {@code ActionInstance} associated with this actionInstanceId.
     * If it has a {@code CronTrigger} then it is also un-scheduled from scheduler
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance deleteActionInstance(String actionInstanceId) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            deleteActionInstance(actionInstance);
        } else {
            throw new ActionInstanceNotFoundException("No actionInstance found with actionInstance id: " + actionInstanceId);
        }
        return actionInstance;
    }

    /**
     * Deletes/Removes the {@code ActionInstance}. If it has a {@code CronTrigger} then it is also un-scheduled from
     * scheduler
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void deleteActionInstance(ActionInstance actionInstance) {
        actionInstanceDao.deleteActionInstance(actionInstance);
        if (actionInstance.getFenzoTrigger() != null) {
            try {
                triggerOperator.deleteTrigger(actionInstance.getFenzoTrigger());
            } catch (SchedulerException e) {
                throw new ActionOperationException(String.format(
                    "Exception occurred while deleting trigger %s for actionInstance %s", actionInstance.getTrigger(), actionInstance), e);
            }
        }
        logger.info("Successfully deleted the actionInstance {}", actionInstance);
    }

    /**
     * Returns a list of {@code ActionInstance}s registered with the actionInstance service for the given actionInstanceGroup
     * @param actionInstanceGroup
     * @return
     */
    public List<ActionInstance> getActionInstances(String actionInstanceGroup) {
        return actionInstanceDao.getActionInstances(actionInstanceGroup);
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
        return executionDao.getExecutions(actionInstanceId);
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
     * Returns a list of {@code Execution}s that are still not complete for a given actionInstance
     * @param actionInstanceId
     * @return
     */
    public List<Execution> getInCompleteExecutionsBefore(String actionInstanceId, Execution currentExecution) {
        List<Execution> executions = executionDao.getExecutions(actionInstanceId);
        List<Execution> incomplete = new ArrayList<>();
        for (Execution execution : executions) {
            if (execution.isBefore(currentExecution) &&
                !currentExecution.getId().equals(execution.getId()) &&
                (execution.getStatus() == null || !execution.getStatus().isComplete())) {
                incomplete.add(execution);
            }
        }
        return incomplete;
    }

    /**
     * @param actionInstanceId
     * @return true if the {@code ActionInstance} has incomplete {@code Execution}s, false otherwise
     */
    public boolean hasInCompleteExecutionsBefore(String actionInstanceId, Execution currentExecution) {
        return getInCompleteExecutionsBefore(actionInstanceId, currentExecution).size() > 0;
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     */
    public Execution execute(String actionInstanceId) throws ActionInstanceNotFoundException {
        return execute(actionInstanceId, DEFAULT_INITIATOR);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public Execution execute(String actionInstanceId, String initiator) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            return execute(actionInstance, initiator);
        }
        throw new ActionInstanceNotFoundException(String.format("No actionInstance found with id: %s", actionInstanceId));
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstance
     */
    public Execution execute(ActionInstance actionInstance) {
        if (!isActionInstanceRegistered(actionInstance)) {
            throw new ExecutionException(String.format("Action instance %s should be registered before executing", actionInstance));
        }
        return execute(actionInstance, DEFAULT_INITIATOR);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstance
     * @param initiator
     * @throws com.netflix.scheduledactions.exceptions.ExecutionException
     */
    public Execution execute(final ActionInstance actionInstance, String initiator)  {

        if (actionInstance.isDisabled()) {
            throw new ExecutionException(String.format("Action instance %s cannot be executed since it is disabled", actionInstance));
        }

        final Execution execution = new Execution(actionInstance.getId());
        executionDao.createExecution(actionInstance.getId(), execution);

        if (hasInCompleteExecutionsBefore(actionInstance.getId(), execution)) {
            ActionInstance.ConcurrentExecutionStrategy strategy = actionInstance.getConcurrentExecutionStrategy();
            switch (strategy) {
                case ALLOW:
                    logger.info("ActionInstance {} concurrent execution strategy is: ALLOW - creating execution", actionInstance);
                    break;
                case REJECT:
                    Status status = Status.SKIPPED;
                    status.setMessage(
                        String.format("ConcurrentExecutionStrategy for ActionInstance %s is REJECT and it has incomplete executions", actionInstance));
                    execution.setStatus(status);
                    execution.setStartTime(new Date());
                    execution.setEndTime(new Date());
                    logger.info("ActionInstance {} concurrent execution strategy is: REJECT - skipping execution", actionInstance);
                    executionDao.updateExecution(execution);
                    return execution;
                case REPLACE:
                    logger.info("ActionInstance {} concurrent execution strategy is: REPLACE - cancelling previous execution(s)", actionInstance);
                    cancelPreviousExecutions(actionInstance, execution);
                    break;
                default:
                    break;
            }
        }

        executeService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Action action = actionInstance.getAction().newInstance();
                    logger.info("Executing action {} for execution {} ...", action, execution);
                    executor.execute(action, actionInstance, execution);
                } catch (InstantiationException | IllegalAccessException e) {
                    Status status = Status.FAILED;
                    status.setMessage(String.format("Exception occurred while creating an action instance of type %s: %s", actionInstance.getAction(), e.getMessage()));
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                } catch (ExecutionException e) {
                    Status status = e.getStatus() != null ? e.getStatus() : Status.FAILED;
                    status.setMessage(e.getMessage());
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                } catch (Exception e) {
                    Status status = Status.FAILED;
                    status.setMessage(String.format("Exception occurred while executing action %s: %s", actionInstance.getAction().getName(), e.getMessage()));
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                } finally {
                    executionDao.updateExecution(execution);
                }
            }
        });

        return execution;
    }

    /**
     * Cancels the currently running {@code Execution} for given {@code ActionInstance} id
     * @param executionId
     * @throws ActionInstanceNotFoundException
     */
    public void cancel(String executionId) throws ExecutionNotFoundException, ActionInstanceNotFoundException {
        Execution execution = executionDao.getExecution(executionId);
        if (execution != null) {
            ActionInstance actionInstance = getActionInstance(execution.getActionInstanceId());
            if (actionInstance != null) {
                cancel(execution, actionInstance);
            } else {
                throw new ExecutionNotFoundException(String.format("No actionInstance found associated with executionId: %s", executionId));
            }
        } else {
            throw new ActionInstanceNotFoundException(String.format("No execution found for executionId: %s", executionId));
        }
    }

    /**
     * Cancels the {@code Execution}
     * @param actionInstance
     */
    public void cancel(final Execution execution, final ActionInstance actionInstance) {
        Status status = execution.getStatus();
        if (status != null && status.isComplete()) {
            throw new ExecutionException(String.format("Action execution %s cannot be cancelled as it already has a %s status", execution, status));
        }
        cancelService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Action action = actionInstance.getAction().newInstance();
                    executor.cancel(action, actionInstance, execution);
                    logger.info("Cancelled action {} for execution {}", action, execution);
                } catch (InstantiationException | IllegalAccessException e) {
                    Status status = Status.FAILED;
                    status.setMessage(String.format("Exception occurred while creating an action instance of type %s: %s", actionInstance.getAction(), e.getMessage()));
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                } catch (ExecutionException e) {
                    Status status = e.getStatus() != null ? e.getStatus() : Status.FAILED;
                    status.setMessage(e.getMessage());
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                } catch (Exception e) {
                    Status status = Status.FAILED;
                    status.setMessage(String.format("Exception occurred while cancelling execution %s: %s", execution, e.getMessage()));
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                } finally {
                    executionDao.updateExecution(execution);
                }
            }
        });
    }

    /**
     * Cancels all {@code Execution}s for this {@code ActionInstance} except for the one passed
     * @param actionInstance
     * @param execution
     */
    public void cancelPreviousExecutions(ActionInstance actionInstance, Execution execution) {
        List<Execution> executions = getInCompleteExecutionsBefore(actionInstance.getId(), execution);
        logger.info("Cancelling following executions for actionInstance {} before {}: {}",
            actionInstance, execution, executions);
        for (Execution incomplete : executions) {
            cancel(incomplete, actionInstance);
        }
    }

}
