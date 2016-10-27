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

package com.netflix.scheduledactions.persistence

import com.netflix.scheduledactions.Execution
import com.netflix.scheduledactions.ActionInstance
import com.netflix.scheduledactions.Status
import spock.lang.Shared
import spock.lang.Specification

class InMemoryDaoSpec extends Specification {

    @Shared ExecutionDao executionDao = new InMemoryExecutionDao()
    @Shared ActionInstanceDao actionInstanceDao = new InMemoryActionInstanceDao()

    void 'test create execution'() {
        when:
        String actionInstanceId = 'foobar1'
        String executionId = executionDao.createExecution(actionInstanceId, new Execution('localExecutorId', actionInstanceId))
        Execution execution = executionDao.getExecution(executionId)

        then:
        execution.id == executionId
        execution.actionInstanceId == actionInstanceId
    }

    void 'test update execution'() {
        when:
        String actionInstanceId = 'foobar1'
        String executionId = executionDao.createExecution(actionInstanceId, new Execution('localExecutorId', actionInstanceId))
        Execution execution = executionDao.getExecution(executionId)

        then:
        execution.id == executionId
        execution.actionInstanceId == actionInstanceId
        execution.status == null

        when:
        execution.status = Status.COMPLETED
        executionDao.updateExecution(execution)
        Execution updatedExecution = executionDao.getExecution(executionId)

        then:
        updatedExecution.id == executionId
        updatedExecution.actionInstanceId == actionInstanceId
        updatedExecution.status == Status.COMPLETED
    }

    void 'test list execution'() {
        when:
        String actionInstanceId = 'foobar1'
        String executionId1 = executionDao.createExecution(actionInstanceId, new Execution('localExecutorId', actionInstanceId))
        String executionId2 = executionDao.createExecution(actionInstanceId, new Execution('localExecutorId', actionInstanceId))
        List<Execution> executions = executionDao.getExecutions(actionInstanceId)

        then:
        executions.find { it.id == executionId1 } != null
        executions.find { it.id == executionId2 } != null
    }

    void 'test create action instance'() {
        when:
        String group = 'api'
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('actionInstance1').build()
        String actionInstanceId = actionInstanceDao.createActionInstance(group, actionInstance)
        ActionInstance savedActionInstance = actionInstanceDao.getActionInstance(actionInstanceId)

        then:
        savedActionInstance.id == actionInstanceId
        savedActionInstance.name == 'actionInstance1'
    }

    void 'test update action instance'() {
        when:
        String group = 'api'
        ActionInstance actionInstance = ActionInstance.newActionInstance().withName('actionInstance1').build()
        String actionInstanceId = actionInstanceDao.createActionInstance(group, actionInstance)
        ActionInstance savedActionInstance = actionInstanceDao.getActionInstance(actionInstanceId)

        then:
        savedActionInstance.id == actionInstanceId
        savedActionInstance.name == 'actionInstance1'
        savedActionInstance.owners == null

        when:
        savedActionInstance.owners = ['sthadeshwar@netflix.com']
        actionInstanceDao.updateActionInstance(savedActionInstance)
        ActionInstance updatedActionInstance = actionInstanceDao.getActionInstance(savedActionInstance.id)

        then:
        updatedActionInstance.id == savedActionInstance.id
        updatedActionInstance.name == 'actionInstance1'
        updatedActionInstance.owners == ['sthadeshwar@netflix.com']
    }

    void 'test list action instance'() {
        when:
        String group = 'api'
        ActionInstance actionInstance1 = ActionInstance.newActionInstance().withName('actionInstance1').build()
        ActionInstance actionInstance2 = ActionInstance.newActionInstance().withName('actionInstance2').build()
        String actionInstanceId1 = actionInstanceDao.createActionInstance(group, actionInstance1)
        String actionInstanceId2 = actionInstanceDao.createActionInstance(group, actionInstance2)
        List<ActionInstance> actionInstances = actionInstanceDao.getActionInstances(group)

        then:
        actionInstances.find { it.id == actionInstanceId1 } != null
        actionInstances.find { it.id == actionInstanceId2 } != null
    }
}
