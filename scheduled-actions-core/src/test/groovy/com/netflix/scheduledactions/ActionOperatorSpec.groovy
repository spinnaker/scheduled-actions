package com.netflix.scheduledactions
import com.netflix.fenzo.triggers.persistence.InMemoryTriggerDao
import com.netflix.scheduledactions.ActionInstance
import com.netflix.scheduledactions.ActionSupport
import com.netflix.scheduledactions.Context
import com.netflix.scheduledactions.CronTrigger
import com.netflix.scheduledactions.DaoConfigurer
import com.netflix.scheduledactions.Execution
import com.netflix.scheduledactions.Status
import com.netflix.scheduledactions.TriggeredActionsOperator
import com.netflix.scheduledactions.exceptions.ExecutionException
import com.netflix.scheduledactions.executors.BlockingThreadPoolLocalExecutor
import com.netflix.scheduledactions.persistence.InMemoryExecutionDao
import com.netflix.scheduledactions.persistence.InMemoryActionInstanceDao
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author sthadeshwar
 */
class ActionOperatorSpec extends Specification {

    @Shared DaoConfigurer daoConfigurer = new DaoConfigurer(new InMemoryActionInstanceDao(), new InMemoryTriggerDao())
    @Shared int threadPoolSize = 10     // Adjust this as per the number of tests
    @Shared TriggeredActionsOperator actionOperator = TriggeredActionsOperator.getInstance(daoConfigurer,
        new BlockingThreadPoolLocalExecutor(new InMemoryExecutionDao(), threadPoolSize), threadPoolSize)

    static class WaitAction extends ActionSupport {
        @Override
        void execute(Context context, Execution execution) throws ExecutionException {
            int doWorkForSeconds = context.parameters ? context.parameters.doWorkForSeconds as Integer : 5
            while (Thread.currentThread().isInterrupted() || --doWorkForSeconds > 0) {
                Thread.sleep(1000L)
            }
        }
    }

    static class FailingAction extends ActionSupport {
        @Override
        void execute(Context context, Execution execution) throws ExecutionException {
            String exceptionMessage = context.parameters ? context.parameters.exceptionMessage : 'Error encountered!'
            throw new ExecutionException(exceptionMessage)
        }
    }

    void 'test register action instance without action class throws exception'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('testRegisterActionInstance').withGroup('ActionOperatorSpec').build()

        when:
        actionOperator.registerActionInstance(actionInstance)

        then:
        thrown(IllegalArgumentException)
    }

    void 'test register action instance without name throws exception'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withAction(WaitAction.class).withGroup('ActionOperatorSpec').build()

        when:
        actionOperator.registerActionInstance(actionInstance)

        then:
        thrown(IllegalArgumentException)
    }

    void 'test register action instance without group sets the default group'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('registerActionInstanceWithoutGroup').withAction(WaitAction.class).build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        ActionInstance regActionInstance = actionOperator.getActionInstance(actionInstanceId)

        then:
        regActionInstance != null
        regActionInstance.group != null && regActionInstance.group.size() > 0
    }

    void 'test delete action instance'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('deleteActionInstance').withAction(WaitAction.class).build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        ActionInstance regActionInstance = actionOperator.getActionInstance(actionInstanceId)

        then:
        regActionInstance != null

        when:
        actionOperator.deleteActionInstance(regActionInstance.id)
        ActionInstance delActionInstance = actionOperator.getActionInstance(regActionInstance.id)

        then:
        delActionInstance == null
    }

    void 'test action execution end to end'() {
        setup:
        Map params = [doWorkForSeconds:10]
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('executeActionInstance')
            .withAction(WaitAction.class)
            .withParameters(params)
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        Execution execution = actionOperator.execute(actionInstanceId)

        then:
        execution != null

        when:
        execution = pollExecutionUntil(execution, Status.IN_PROGRESS, 5)

        then:
        execution.status == Status.IN_PROGRESS
        execution.startTime != null
        execution.endTime == null
        actionInstance.disabled == false

        when:
        List<Execution> executions = actionOperator.getExecutions(actionInstanceId)

        then:
        executions != null && executions.find { it.id == execution.id } != null

        when:
        execution = pollExecutionUntil(execution, Status.COMPLETED, 15)

        then:
        execution.status == Status.COMPLETED
        execution.endTime != null
    }

    void 'executing a disabled action instance does not create an action execution'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('disabledActionInstance')
            .withAction(WaitAction.class)
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        actionOperator.disableActionInstance(actionInstanceId)
        actionOperator.execute(actionInstance)

        then:
        thrown(ExecutionException)
    }

    void 'if action takes longer execute than the specified timeout then the status is marked as TIMED_OUT'() {
        setup:
        Map params = [doWorkForSeconds:10]
        int timeoutInSeconds = 2
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('executeActionInstance')
            .withAction(WaitAction.class)
            .withParameters(params)
            .withExecutionTimeoutInSeconds(timeoutInSeconds)
            .build()

        when:
        actionOperator.registerActionInstance(actionInstance)
        Execution execution = actionOperator.execute(actionInstance)

        then:
        execution != null

        when:
        execution = pollExecutionUntil(execution, Status.TIMED_OUT, 5)

        then:
        execution.status == Status.TIMED_OUT
        execution.status.message == "Action ${WaitAction.class.name} timed out after ${timeoutInSeconds} seconds"
        execution.startTime != null
        execution.endTime != null
        actionInstance.disabled == false
    }

    void 'if an action fails the status is marked as FAILED'() {
        setup:
        Map params = [exceptionMessage: 'Action Failed!']
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('failedActionInstance')
            .withAction(FailingAction.class)
            .withParameters(params)
            .build()

        when:
        actionOperator.registerActionInstance(actionInstance)
        Execution execution = actionOperator.execute(actionInstance)

        then:
        execution != null

        when:
        execution = pollExecutionUntil(execution, Status.FAILED, 5)

        then:
        execution.status == Status.FAILED
        execution.status.message == "Exception occurred in action ${FailingAction.class.name}: ${params.exceptionMessage}"
        execution.startTime != null
        execution.endTime != null
    }

    void 'cancelling an action execution should cancel the running action'() {
        setup:
        Map params = [doWorkForSeconds:30]
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('executeActionInstance')
            .withAction(WaitAction.class)
            .withParameters(params)
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        Execution execution = actionOperator.execute(actionInstanceId)

        then:
        execution != null

        when:
        execution = pollExecutionUntil(execution, Status.IN_PROGRESS, 5)

        then:
        execution.status == Status.IN_PROGRESS

        when:
        actionOperator.cancel(execution.id)
        execution = pollExecutionUntil(execution, Status.CANCELLED, 5)

        then:
        execution.status == Status.CANCELLED
        execution.startTime != null
        execution.endTime != null
    }

    void 'if execution is in progress and action instance has REJECT strategy then subsequent executions should be skipped'() {
        setup:
        Map params = [doWorkForSeconds:10]
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('actionInstance')
            .withAction(WaitAction.class)
            .withParameters(params)
            .withConcurrentExecutionStrategy(ActionInstance.ConcurrentExecutionStrategy.REJECT)
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        Execution execution1 = actionOperator.execute(actionInstanceId)
        Execution execution2 = actionOperator.execute(actionInstanceId)

        then:
        execution1 != null

        when:
        execution1 = pollExecutionUntil(execution1, Status.IN_PROGRESS, 5)

        then:
        execution1.status == Status.IN_PROGRESS
        execution1.startTime != null
        execution1.endTime == null

        execution2 != null
        execution2.status == Status.SKIPPED
        execution2.startTime != null
        execution2.endTime != null
    }

    void 'if execution is in progress and action instance has ALLOW strategy then subsequent executions should be allowed'() {
        setup:
        Map params = [doWorkForSeconds:10]
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('actionInstance')
            .withAction(WaitAction.class)
            .withParameters(params)
            .withConcurrentExecutionStrategy(ActionInstance.ConcurrentExecutionStrategy.ALLOW)
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        Execution execution1 = actionOperator.execute(actionInstanceId)
        Execution execution2 = actionOperator.execute(actionInstanceId)

        then:
        execution1 != null
        execution2 != null

        when:
        execution1 = pollExecutionUntil(execution1, Status.IN_PROGRESS, 5)
        execution2 = pollExecutionUntil(execution2, Status.IN_PROGRESS, 5)

        then:
        execution1.status == Status.IN_PROGRESS
        execution1.startTime != null
        execution1.endTime == null
        execution2.status == Status.IN_PROGRESS
        execution2.startTime != null
        execution2.endTime == null

        when:
        execution1 = pollExecutionUntil(execution1, Status.COMPLETED, 20)
        execution2 = pollExecutionUntil(execution2, Status.COMPLETED, 20)

        then:
        execution1.status == Status.COMPLETED
        execution1.startTime != null
        execution1.endTime != null
        execution2.status == Status.COMPLETED
        execution2.startTime != null
        execution2.endTime != null
    }

    void 'if execution is in progress and action instance has REPLACE strategy then subsequent executions should cancel previous executions'() {
        setup:
        Map params = [doWorkForSeconds:10]
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('actionInstance')
            .withAction(WaitAction.class)
            .withParameters(params)
            .withConcurrentExecutionStrategy(ActionInstance.ConcurrentExecutionStrategy.REPLACE)
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)
        Execution execution1 = actionOperator.execute(actionInstanceId)
        Execution execution2 = actionOperator.execute(actionInstanceId)

        then:
        execution1 != null
        execution1 != null

        when:
        Thread.sleep(20*1000L)
        execution1 = actionOperator.getExecution(execution1.id)
        execution2 = actionOperator.getExecution(execution2.id)

        then:
        execution1.status == Status.CANCELLED
        execution2.status == Status.COMPLETED
    }

    void 'a failed action execution should not affect the next action execution'() {
        setup:
        actionOperator.initialize()
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('failedActionInstance')
            .withAction(FailingAction.class)
            .withTrigger(new CronTrigger('0/5 * * * * ? *'))
            .build()

        when:
        String actionInstanceId = actionOperator.registerActionInstance(actionInstance)

        then:
        actionInstanceId != null

        when:
        List<Execution> executions = pollExecutionsUntilCount(actionInstanceId, 2, 15)

        then:
        executions != null
        executions.size() >= 2
        executions.findAll { it.status == Status.FAILED }?.size() >= 2
    }


    private Execution pollExecutionUntil(Execution execution, Status expectedStatus, int timeoutInSeconds) {
        Execution updated = execution
        try {
            while (--timeoutInSeconds >= 0) {
                updated = actionOperator.getExecution(execution.id)
                if (updated.status == expectedStatus) break
                else Thread.sleep(1000L)
            }
        } catch (e) {}
        return updated
    }

    private List<Execution> pollExecutionsUntilCount(String actionInstanceId, int count, int timeoutInSeconds) {
        List<Execution> executions
        try {
            while (--timeoutInSeconds >= 0) {
                executions = actionOperator.getExecutions(actionInstanceId)
                if (executions.size() >= count) break
                else Thread.sleep(1000L)
            }
        } catch (e) {}
        return executions
    }
}
