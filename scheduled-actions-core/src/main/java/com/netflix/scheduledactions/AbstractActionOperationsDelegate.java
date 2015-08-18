/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.scheduledactions;

import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException;
import com.netflix.scheduledactions.exceptions.ActionOperationException;
import com.netflix.scheduledactions.exceptions.ExecutionException;
import com.netflix.scheduledactions.exceptions.ExecutionNotFoundException;
import com.netflix.scheduledactions.executors.Executor;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import com.netflix.scheduledactions.persistence.ExecutionDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author sthadeshwar
 */
public class AbstractActionOperationsDelegate implements ActionOperationsDelegate {

    private static final Logger logger = LoggerFactory.getLogger(DefaultActionOperationsDelegate.class);

    protected final TriggerOperator triggerOperator;
    protected final ActionInstanceDao actionInstanceDao;
    protected final ExecutionDao executionDao;
    protected final Executor executor;
    protected final String delegateId;
    protected final ExecutorService executeService;
    protected final ExecutorService cancelService;

    private static ActionOperationsDelegate actionOperationsDelegate;

    public AbstractActionOperationsDelegate(String delegateId,
                                           TriggerOperator triggerOperator,
                                           DaoConfigurer daoConfigurer,
                                           Executor executor,
                                           int threadPoolSize) {
        this.triggerOperator = triggerOperator;
        this.actionInstanceDao = daoConfigurer.getActionInstanceDao();
        this.executionDao = daoConfigurer.getExecutionDao();
        this.executor = executor;
        this.delegateId = delegateId;
        this.executeService = Executors.newFixedThreadPool(threadPoolSize);
        this.cancelService = Executors.newFixedThreadPool(threadPoolSize > 1 ? threadPoolSize/2 : threadPoolSize);
    }

    @Override
    public void initialize() {
        try {
            actionOperationsDelegate = this;
            this.triggerOperator.initialize();
        } catch (SchedulerException e) {
            throw new RuntimeException("Exception occurred while initializing AbstractActionOperationsDelegate", e);
        }
    }

    @Override
    public void destroy() {
        try {
            this.triggerOperator.destroy();
        } catch (SchedulerException e) {
            throw new RuntimeException("Exception occurred while destroying AbstractActionOperationsDelegate", e);
        }
    }

    @Override
    public boolean isClustered() {
        return false;
    }

    /**
     * Registers a {@code ActionInstance} with actionInstance service
     * @param actionInstance
     */
    @Override
    public String register(ActionInstance actionInstance) {
        validate(actionInstance);

        actionInstanceDao.createActionInstance(actionInstance.getGroup(), actionInstance);
        if (actionInstance.getTrigger() != null) {
            actionInstance.setFenzoTrigger(actionInstance.getTrigger().createFenzoTrigger(actionInstance.getContext(),
                InternalAction.class));
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
        @Override
        public void call(Context context) {
            String actionInstanceId = context.getActionInstanceId();
            try {
                logger.info("[{}] InternalAction: Calling execute() on delegate with context: {}", actionInstanceId, context);
                actionOperationsDelegate.execute(actionInstanceId, "ScheduledTrigger"); // TODO: Pass trigger info from context
            } catch (ActionInstanceNotFoundException e) {}
        }
    }

    /**
     *
     * @param actionInstance
     */
    @Override
    public void update(ActionInstance actionInstance) {
        // Find an existing one to update
        ActionInstance existingInstance = actionInstanceDao.getActionInstance(actionInstance.getId());
        if (existingInstance == null) {
            throw new ActionOperationException(
                String.format("No existing actionInstance with id %s found for the update operation", actionInstance.getId())
            );
        }
        // Set the id of the existing action instance so that this looks like an "update" operation
        actionInstance.setId(existingInstance.getId());

        // Validate the new one before deleting the existing one
        validate(actionInstance);

        // Delete the existing one
        delete(existingInstance);

        // Register the new one
        this.register(actionInstance);
        logger.info("Successfully updated the actionInstance {}", actionInstance);
    }

    /**
     *
     * @param actionInstance
     */
    @Override
    public void validate(ActionInstance actionInstance) {
        if (actionInstance == null) {
            throw new IllegalArgumentException("actionInstance cannot be null");
        }
        if (actionInstance.getName() == null || "".equals(actionInstance.getName())) {
            throw new IllegalArgumentException("name for the actionInstance cannot be null or empty");
        }
        if (actionInstance.getAction() == null) {
            throw new IllegalArgumentException("No Action class specified for the actionInstance");
        }
        if (actionInstance.getTrigger() != null) {
            actionInstance.getTrigger().validate();
        }
    }

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    @Override
    public ActionInstance disable(String actionInstanceId) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            disable(actionInstance);
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
    @Override
    public void disable(ActionInstance actionInstance) {
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
    @Override
    public ActionInstance enable(String actionInstanceId) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            enable(actionInstance);
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
    @Override
    public void enable(ActionInstance actionInstance) {
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
    @Override
    public ActionInstance delete(String actionInstanceId) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (actionInstance != null) {
            delete(actionInstance);
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
    @Override
    public void delete(ActionInstance actionInstance) {
        actionInstanceDao.deleteActionInstance(actionInstance.getGroup(), actionInstance);
        if (actionInstance.getFenzoTrigger() != null) {
            try {
                triggerOperator.deleteTrigger(actionInstance.getGroup(), actionInstance.getFenzoTrigger());
            } catch (SchedulerException e) {
                throw new ActionOperationException(String.format(
                    "Exception occurred while deleting trigger %s for actionInstance %s", actionInstance.getTrigger(), actionInstance), e);
            }
        }
        logger.info("Successfully deleted the actionInstance {}", actionInstance);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstanceId
     * @throws com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    @Override
    public Execution execute(String actionInstanceId, String initiator) throws ActionInstanceNotFoundException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (actionInstance == null) {
            throw new ActionInstanceNotFoundException(String.format("No actionInstance found with id: %s", actionInstanceId));
        }
        return execute(actionInstance, initiator);
    }

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstance
     * @param initiator
     * @throws com.netflix.scheduledactions.exceptions.ExecutionException
     */
    public Execution execute(final ActionInstance actionInstance, String initiator)  {

        if (actionInstance.isDisabled()) {
            return null;
        }

        final String actionInstanceId = actionInstance.getId();
        final Execution execution = new Execution(delegateId, actionInstanceId);
        final String executionId = executionDao.createExecution(actionInstanceId, execution);

        logger.info("[{}] Created execution for actionInstance: {}", actionInstanceId, executionId);
        execution.getLogger().info(String.format("Created execution %s", executionId));

        List<Execution> previousExecutions = getInCompleteExecutionsBefore(actionInstanceId, execution);
        if (previousExecutions.size() > 0) {
            ConcurrentExecutionStrategy strategy = actionInstance.getConcurrentExecutionStrategy();
            switch (strategy) {
                case ALLOW:
                    execution.getLogger().info("Concurrent execution strategy is: ALLOW, allowing execution...");
                    logger.info("[{}] actionInstance concurrent execution strategy is: ALLOW, allowing execution...",
                        actionInstanceId);
                    break;
                case REJECT:
                    Status status = Status.SKIPPED;
                    status.setMessage(
                        String.format(
                            "ConcurrentExecutionStrategy for ActionInstance %s is REJECT and it has incomplete executions",
                            actionInstance
                        )
                    );
                    execution.setStatus(status);
                    execution.setStartTime(new Date());
                    execution.setEndTime(new Date());
                    logger.info("[{}] actionInstance concurrent execution strategy is: REJECT, skipping execution",
                        actionInstanceId);
                    execution.getLogger().info("Concurrent execution strategy is: REJECT, skipping execution");
                    executionDao.updateExecution(execution);
                    return execution;
                case REPLACE:
                    logger.info("[{}] actionInstance concurrent execution strategy is: REPLACE, cancelling previous execution(s)",
                        actionInstanceId);
                    cancelPreviousExecutions(actionInstance, execution);
                    break;
                default:
                    break;
            }
        }

        logger.info("[{}] Submitting runnable for execution: {}", actionInstanceId, executionId);

        executeService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Action action = newInstance(actionInstance);
                    execution.getLogger().info("Calling executor.execute()...");
                    logger.info("[{}] Calling executor.execute() for execution {} ...", actionInstanceId, executionId);
                    executor.execute(action, actionInstance, execution);
                } catch (ExecutionException e) {
                    Status status = e.getStatus() != null ? e.getStatus() : Status.FAILED;
                    status.setMessage(e.getMessage());
                    execution.setEndTime(new Date());
                    execution.setStatus(status);
                    execution.getLogger().error(
                        String.format("Exception occurred while executing action: %s", e.getMessage())
                    );
                } catch (Exception e) {
                    Status status = Status.FAILED;
                    status.setMessage(
                        String.format(
                            "Exception occurred while executing action %s: %s", actionInstance.getAction(), e.getMessage()
                        )
                    );
                    execution.getLogger().error(
                        String.format("Exception occurred while executing action: %s", e.getMessage())
                    );
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
    @Override
    public void cancel(String executionId) throws ExecutionNotFoundException, ActionInstanceNotFoundException {
        Execution execution = executionDao.getExecution(executionId);
        if (execution != null) {
            ActionInstance actionInstance = actionInstanceDao.getActionInstance(execution.getActionInstanceId());
            if (actionInstance != null) {
                cancel(execution, actionInstance);
            } else {
                throw new ExecutionNotFoundException(String.format("No actionInstance found associated with executionId: %s", executionId));
            }
        } else {
            throw new ActionInstanceNotFoundException(String.format("No execution found for executionId: %s", executionId));
        }
    }

    @Override
    public void cancel(final Execution execution, final ActionInstance actionInstance) {
        cancelLocal(execution, actionInstance);
    }

    protected void cancelLocal(final Execution execution, final ActionInstance actionInstance) {
        Execution updatedExecution = executionDao.getExecution(execution.getId());
        Status status = updatedExecution.getStatus();

        if (status != null && status.isComplete()) {
            return;
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
    private void cancelPreviousExecutions(ActionInstance actionInstance, Execution execution) {
        List<Execution> executions = getInCompleteExecutionsBefore(actionInstance.getId(), execution);
        logger.info("Cancelling following executions for actionInstance {} before {}: {}",
            actionInstance, execution, executions);
        for (Execution incomplete : executions) {
            cancel(incomplete, actionInstance);
        }
    }

    /**
     * Returns a list of {@code Execution}s that are still not complete for a given actionInstance
     * @param actionInstanceId
     * @return
     */
    private List<Execution> getInCompleteExecutionsBefore(String actionInstanceId, Execution currentExecution) {
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
     *
     * @return
     */
    protected Action newInstance(ActionInstance actionInstance) {
        try {
            return actionInstance.getAction().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Exception occurred while creating an action instance of type %s: %s", actionInstance.getAction(), e.getMessage()));
        }
    }
}
