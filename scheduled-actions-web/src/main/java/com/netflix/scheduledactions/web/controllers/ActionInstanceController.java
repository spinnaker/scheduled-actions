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

package com.netflix.scheduledactions.web.controllers;

import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.ActionsOperator;
import com.netflix.scheduledactions.Execution;
import com.netflix.scheduledactions.exceptions.ExecutionNotFoundException;
import com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 *
 * @author sthadeshwar
 */
@RestController
@ConditionalOnBean(ActionsOperator.class)
public class ActionInstanceController {

    @Autowired
    ActionsOperator actionsOperator;

    @RequestMapping(value = "/scheduledActions", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    public String createActionInstance(@RequestBody ActionInstance actionInstance) {
        return actionsOperator.registerActionInstance(actionInstance);
    }

    @RequestMapping(value = "/scheduledActions/{id}/execute", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Execution executeAction(@PathVariable String id) throws ActionInstanceNotFoundException {
        return actionsOperator.execute(id);
    }

    @RequestMapping(value = "/scheduledActions/{id}/disable", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ActionInstance disableActionInstance(@PathVariable String id) throws ActionInstanceNotFoundException {
        return actionsOperator.disableActionInstance(id);
    }

    @RequestMapping(value = "/scheduledActions/{id}/enable", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ActionInstance enableActionInstance(@PathVariable String id) throws ActionInstanceNotFoundException {
        return actionsOperator.enableActionInstance(id);
    }

    @RequestMapping(value = "/scheduledActions", method = RequestMethod.GET)
    public List<ActionInstance> actionInstances(@RequestParam String actionInstanceGroup) {
        return actionsOperator.getActionInstances(actionInstanceGroup);
    }

    @RequestMapping(value = "/scheduledActions/{id}", method = RequestMethod.GET)
    public ActionInstance actionInstance(@PathVariable String id) {
        return actionsOperator.getActionInstance(id);
    }

    @RequestMapping(value = "/scheduledActions/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ActionInstance deleteActionInstance(@PathVariable String id) throws ActionInstanceNotFoundException {
        return actionsOperator.deleteActionInstance(id);
    }

    @RequestMapping(value = "/scheduledActions/{id}/executions", method = RequestMethod.GET)
    public List<Execution> executions(@PathVariable String id) {
        return actionsOperator.getExecutions(id);
    }

    @RequestMapping(value = "/scheduledActions/executions/{id}", method = RequestMethod.GET)
    public Execution execution(@PathVariable String id) {
        return actionsOperator.getExecution(id);
    }

    @RequestMapping(value = "/scheduledActions/executions/{id}/cancel", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void cancelExecution(@PathVariable String id) throws ActionInstanceNotFoundException, ExecutionNotFoundException {
        actionsOperator.cancel(id);
    }
}
