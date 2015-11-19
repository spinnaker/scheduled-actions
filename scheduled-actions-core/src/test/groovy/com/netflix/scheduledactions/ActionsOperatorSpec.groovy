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

package com.netflix.scheduledactions
import com.netflix.fenzo.triggers.TriggerOperator
import com.netflix.fenzo.triggers.persistence.InMemoryTriggerDao
import com.netflix.scheduledactions.exceptions.ExecutionException
import com.netflix.scheduledactions.persistence.InMemoryActionInstanceDao
import com.netflix.scheduledactions.persistence.InMemoryExecutionDao
import com.netflix.scheduledactions.triggers.CronTrigger
import spock.lang.Shared
import spock.lang.Specification
class ActionsOperatorSpec extends Specification {

    @Shared DaoConfigurer daoConfigurer = new DaoConfigurer(new InMemoryActionInstanceDao(), new InMemoryTriggerDao(), new InMemoryExecutionDao())
    @Shared int threadPoolSize = 10     // Adjust this as per the number of tests
    @Shared ActionsOperator actionsOperator = new ActionsOperator(
        TriggerOperator.getInstance(daoConfigurer.triggerDao, threadPoolSize),
        daoConfigurer,
        threadPoolSize
    )

    def setupSpec() {
        actionsOperator.initialize()
    }

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
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('testRegisterActionInstance').withGroup('ActionsOperatorSpec').build()

        when:
        actionsOperator.registerActionInstance(actionInstance)

        then:
        thrown(IllegalArgumentException)
    }

    void 'test register action instance without name throws exception'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withAction(WaitAction.class).withGroup('ActionsOperatorSpec').build()

        when:
        actionsOperator.registerActionInstance(actionInstance)

        then:
        thrown(IllegalArgumentException)
    }

    void 'test register action instance without group sets the default group'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('registerActionInstanceWithoutGroup').withAction(WaitAction.class).build()

        when:
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        ActionInstance regActionInstance = actionsOperator.getActionInstance(actionInstanceId)

        then:
        regActionInstance != null
        regActionInstance.group != null && regActionInstance.group.size() > 0
    }

    void 'test delete action instance'() {
        setup:
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('deleteActionInstance').withAction(WaitAction.class).build()

        when:
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        ActionInstance regActionInstance = actionsOperator.getActionInstance(actionInstanceId)

        then:
        regActionInstance != null

        when:
        actionsOperator.deleteActionInstance(regActionInstance.id)
        ActionInstance delActionInstance = actionsOperator.getActionInstance(regActionInstance.id)

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
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        Execution execution = actionsOperator.execute(actionInstanceId)

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
        List<Execution> executions = actionsOperator.getExecutions(actionInstanceId)

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
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        actionsOperator.disableActionInstance(actionInstanceId)
        Execution execution = actionsOperator.execute(actionInstance)
        List executions = actionsOperator.getExecutions(actionInstanceId)

        then:
        execution == null
        executions == null || executions.size() == 0
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
        actionsOperator.registerActionInstance(actionInstance)
        Execution execution = actionsOperator.execute(actionInstance)

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
        actionsOperator.registerActionInstance(actionInstance)
        Execution execution = actionsOperator.execute(actionInstance)

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
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        Execution execution = actionsOperator.execute(actionInstanceId)

        then:
        execution != null

        when:
        execution = pollExecutionUntil(execution, Status.IN_PROGRESS, 5)

        then:
        execution.status == Status.IN_PROGRESS

        when:
        actionsOperator.cancel(execution.id)
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
            .withConcurrentExecutionStrategy(ConcurrentExecutionStrategy.REJECT)
            .build()

        when:
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        Execution execution1 = actionsOperator.execute(actionInstanceId)
        Execution execution2 = actionsOperator.execute(actionInstanceId)

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
            .withConcurrentExecutionStrategy(ConcurrentExecutionStrategy.ALLOW)
            .build()

        when:
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        Execution execution1 = actionsOperator.execute(actionInstanceId)
        Execution execution2 = actionsOperator.execute(actionInstanceId)

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
            .withConcurrentExecutionStrategy(ConcurrentExecutionStrategy.REPLACE)
            .build()

        when:
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)
        Execution execution1 = actionsOperator.execute(actionInstanceId)
        Execution execution2 = actionsOperator.execute(actionInstanceId)

        then:
        execution1 != null
        execution1 != null

        when:
        Thread.sleep(20*1000L)
        execution1 = actionsOperator.getExecution(execution1.id)
        execution2 = actionsOperator.getExecution(execution2.id)

        then:
        execution1.status == Status.CANCELLED
        execution2.status == Status.COMPLETED
    }

    void 'a failed action execution should not affect the next action execution'() {
        setup:
        actionsOperator.initialize()
        ActionInstance actionInstance = ActionInstance.newActionInstance()
            .withName('failedActionInstance')
            .withAction(FailingAction.class)
            .withTrigger(new CronTrigger('0/5 * * * * ? *'))
            .build()

        when:
        String actionInstanceId = actionsOperator.registerActionInstance(actionInstance)

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
                updated = actionsOperator.getExecution(execution.id)
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
                executions = actionsOperator.getExecutions(actionInstanceId)
                if (executions.size() >= count) break
                else Thread.sleep(1000L)
            }
        } catch (e) {}
        return executions
    }
}
