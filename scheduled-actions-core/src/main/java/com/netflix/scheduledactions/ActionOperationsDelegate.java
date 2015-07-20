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

import com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException;
import com.netflix.scheduledactions.exceptions.ExecutionNotFoundException;

/**
 * An operations (create, delete, disable, enable, etc.) delegate for {@code ActionInstance} used
 * by {@code ActionOperator}
 * @author sthadeshwar
 */
public interface ActionOperationsDelegate {

    /**
     * Called when (@code ActionsOperator} is initialized
     */
    public void initialize();

    /**
     * Called when (@code ActionsOperator} is destroyed
     */
    public void destroy();

    /**
     * Indicates whether this delegate can handle operations in a clustered environment or not
     * @return
     */
    public boolean isClustered();

    /**
     * Registers a {@code ActionInstance} with actionInstance service
     * @param actionInstance
     */
    public String register(ActionInstance actionInstance);

    /**
     * Updates an existing actionInstance
     * @param actionInstance
     */
    public void update(ActionInstance actionInstance);
    /**
     *
     * @param actionInstance
     */
    public void validate(ActionInstance actionInstance);

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance disable(String actionInstanceId) throws ActionInstanceNotFoundException;

    /**
     * Disables the {@code ActionInstance}. If the {@code ActionInstance} is disabled it will NOT execute
     * @param actionInstance
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void disable(ActionInstance actionInstance);

    /**
     * Enables the {@code ActionInstance} associated with this actionInstanceId
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance enable(String actionInstanceId) throws ActionInstanceNotFoundException;

    /**
     * Enables the {@code ActionInstance}
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void enable(ActionInstance actionInstance);

    /**
     * Deletes/Removes the {@code ActionInstance} associated with this actionInstanceId.
     * If it has a {@code CronTrigger} then it is also un-scheduled from scheduler
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public ActionInstance delete(String actionInstanceId) throws ActionInstanceNotFoundException;

    /**
     * Deletes/Removes the {@code ActionInstance}. If it has a {@code CronTrigger} then it is also un-scheduled from
     * scheduler
     * @param actionInstance
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public void delete(ActionInstance actionInstance);

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstanceId
     * @throws ActionInstanceNotFoundException
     * @throws com.netflix.scheduledactions.exceptions.ActionOperationException
     */
    public Execution execute(String actionInstanceId, String initiator) throws ActionInstanceNotFoundException;

    /**
     * Executes the {@code ActionInstance}
     * @param actionInstance
     * @param initiator
     * @throws com.netflix.scheduledactions.exceptions.ExecutionException
     */
    public Execution execute(ActionInstance actionInstance, String initiator);

    /**
     * Cancels the currently running {@code Execution} for given {@code ActionInstance} id
     * @param executionId
     * @throws ActionInstanceNotFoundException
     */
    public void cancel(String executionId) throws ExecutionNotFoundException, ActionInstanceNotFoundException;

    /**
     * Cancels the {@code Execution}
     * @param actionInstance
     */
    public void cancel(Execution execution, ActionInstance actionInstance);

}
