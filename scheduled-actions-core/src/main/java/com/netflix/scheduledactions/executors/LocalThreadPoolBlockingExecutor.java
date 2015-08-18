package com.netflix.scheduledactions.executors;

import com.netflix.scheduledactions.*;
import com.netflix.scheduledactions.exceptions.ExecutionException;
import com.netflix.scheduledactions.persistence.ExecutionDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.*;

/**
 * @author sthadeshwar
 */
public class LocalThreadPoolBlockingExecutor implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(LocalThreadPoolBlockingExecutor.class);

    private final ExecutionDao executionDao;
    private final ExecutorService executeService;
    private final ExecutorService cancelService;
    private final ConcurrentMap<String,Future> futures = new ConcurrentHashMap<String, Future>();

    public LocalThreadPoolBlockingExecutor(ExecutionDao executionDao, int threadPoolSize) {
        this.executionDao = executionDao;
        this.executeService = Executors.newFixedThreadPool(threadPoolSize);
        this.cancelService = Executors.newFixedThreadPool(threadPoolSize > 1 ? threadPoolSize/2 : threadPoolSize);
    }

    @Override
    public void execute(final Action action,
                        final ActionInstance actionInstance,
                        final Execution execution) throws ExecutionException {

        final Context context = actionInstance.getContext();
        final ExecutionListener executionListener;
        try {
            executionListener = actionInstance.getExecutionListener().newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new ExecutionException("Exception occurred while instantiating executionListener", e);
        }

        Future future = executeService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    executionListener.onStart(context, execution);
                    execution.setStartTime(new Date());
                    execution.setStatus(Status.IN_PROGRESS);
                    executionDao.updateExecution(execution);

                    // Executing the action
                    logger.info("[{}] Execution started for action {}", actionInstance.getId(),
                        action.getClass().getSimpleName());
                    execution.getLogger().info(
                        String.format("Execution started for action %s", action.getClass().getSimpleName())
                    );
                    action.execute(context, execution);

                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
            }
        });

        registerFuture(execution, future);

        long timeoutInSeconds = actionInstance.getExecutionTimeoutInSeconds();
        try {
            if (timeoutInSeconds > 0) {
                future.get(timeoutInSeconds, TimeUnit.SECONDS);
            } else {
                future.get();
            }

            // Action execution complete
            execution.setEndTime(new Date());
            execution.setStatus(action.getStatus() != null ? action.getStatus() : Status.COMPLETED);
            logger.info("[{}] Execution completed for action {} with status: {}", actionInstance.getId(),
                action.getClass().getSimpleName(), execution.getStatus());
            execution.getLogger().info(
                String.format("Execution completed for action %s with status: %s",
                    action.getClass().getSimpleName(), execution.getStatus())
            );
            executionDao.updateExecution(execution);

            executionListener.onComplete(context, execution);

        } catch (Exception e) {

            Throwable caughtThrowable = e.getCause() != null ? e.getCause().getCause() != null ? e.getCause().getCause(): e.getCause() : e;
            ExecutionException executionException;

            if (caughtThrowable instanceof TimeoutException) {
                executionException = new ExecutionException(String.format("Action %s timed out after %d seconds", action.getClass().getName(), timeoutInSeconds), caughtThrowable, Status.TIMED_OUT);
                executionListener.onError(context, execution);
            } else if (caughtThrowable instanceof CancellationException) {
                executionException = new ExecutionException(String.format("Action %s has been cancelled", action.getClass().getName(), timeoutInSeconds), caughtThrowable, Status.CANCELLED);
            } else {
                executionException = new ExecutionException(String.format("Exception occurred in action %s: %s", action.getClass().getName(), caughtThrowable.getMessage()), caughtThrowable);
                executionListener.onError(context, execution);
            }

            throw executionException;
        }
    }

    @Override
    public void cancel(final Action action,
                       final ActionInstance actionInstance,
                       final Execution execution) throws ExecutionException {

        final ExecutionListener executionListener;
        try {
            executionListener = actionInstance.getExecutionListener().newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new ExecutionException("Exception occurred while instantiating executionListener", e);
        }

        final Context context = actionInstance.getContext();

        cancelService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    executionListener.beforeCancel(context, execution);
                    Future future = getFuture(execution);
                    if (future != null) {
                        future.cancel(true);
                    }
                } catch (Exception e) {
                    throw new ExecutionException(e);
                } finally {
                    execution.setEndTime(new Date());
                    execution.setStatus(Status.CANCELLED);
                    executionDao.updateExecution(execution);
                    logger.info("Successfully cancelled the action {} for execution {}", action, execution);
                }
            }
        });
    }

    private void registerFuture(Execution execution, Future future) {
        futures.put(execution.getId(), future);
    }

    private Future getFuture(Execution execution) {
        return futures.remove(execution.getId());
    }
}
