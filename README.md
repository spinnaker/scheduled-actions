A library for management, operations and persistence of scheduled actions
=

Below are the features:

* Create and register an ```com.netflix.scheduledactions.ActionInstance``` which gives a handle to the implemented action (```com.netflix.scheduledactions.Action```)
* Actions can be scheduled using a type of ```com.netflix.scheduledactions.Trigger```. For example, a scheduled trigger for an action can be a cron expression
* Track and monitor the action executions with ability to add a listener for events like onCancel(), onStart(), onComplete(), onError(), etc.
* ```Action```s can be executed in an ad-hoc manner too.
* ```Action```s can provide cancel hook using the onCancel() callback of an ```com.netflix.scheduledactions.ExecutionListener```
* Enable/Disable ```ActionInstance```s and corresponding scheduled triggers
* Persist and view a history of ```execution```s for a given ```ActionInstance``` using ```scheduled-actions-cassandra``` module

This library is *not* a workflow engine *nor* a scheduling library (It uses ```fenzo-triggers``` library for time based scheduling).

Installation
=

Following are the two artifacts:

| Group/Org                     | Artifact Id                   | Required | Features                                                           |
| ----------------------------- | ----------------------------- | -------- | ------------------------------------------------------------------ |
| com.netflix.scheduledactions  | scheduled-actions-core        | Yes      | Core library features                                              |
| com.netflix.scheduledactions  | scheduled-actions-cassandra   | No       | Provides cassandra persistence for ActionInstance, Execution, etc. |
| com.netflix.scheduledactions  | scheduled-actions-web         | No       | Provides spring REST controller to access the ```ActionsOperator``` |

Usage
=

```com.netflix.scheduledactions.ActionsOperator``` is the primary class for registering, enabling, disabling, executing and cancelling ```ActionInstance```s.

#### Implement an Action ####

Create an action by either implementing ```Action``` interface or by extending ```ActionSupport``` class

```java
public class MyAction implements Action {
    // Implement methods here...
}
```

```java
public class MyAction extends ActionSupport {
    // Implement or/and override methods here...
}
```

#### Create an ActionInstance ####

```ActionInstance``` provides a builder to create an action instance

```java
ActionInstance actionInstance = ActionInstance.newActionInstance()
    .withName("Process Items")
    .withGroup("MyApplication")
    .withAction(MyAction.class)
    .withOwners("sthadeshwar@netflix.com")
    .withWatchers("foobar-team@netflix.com")
    .build();
```

A ```Trigger``` can be associated with a ```ActionInstance```

```java
ActionInstance actionInstance = ActionInstance.newActionInstance()
    .withName("Process Items")
    .withGroup("MyApplication")
    .withAction(MyAction.class)
    .withTrigger(new CronTrigger("0 0 0/1 * * ?"))  // Run the action every hour
    .build();
```

Specify a timeout for your action execution

```java
ActionInstance actionInstance = ActionInstance.newActionInstance()
    .withName("Process Items")
    .withGroup("MyApplication")
    .withAction(MyAction.class)
    .withExecutionTimeoutInSeconds(45*60L)  // Timeout after 45 minutes
    .build();
```

Specify an ```ExecutionListener``` for your action

```java
MyListener implements ExecutionListener {
    // Implement methods here...
}

ActionInstance actionInstance = ActionInstance.newActionInstance()
    .withName("Process Items")
    .withGroup("MyApplication")
    .withAction(MyAction.class)
    .withExecutionListener(new MyListener())
    .build();
```

A concurrent execution strategy can also be setup for the action instance. The strategy can be set to one of the following:
* REJECT - skip the action execution if one is already running (default)
* ALLOW - execute all ```Execution```s concurrently
* REPLACE - cancel the previous one and run the new one

This can be configured while creating the ```ActionInstance```

```java
ActionInstance actionInstance = ActionInstance.newActionInstance()
    .withName("Process Items")
    .withGroup("MyApplication")
    .withAction(MyAction.class)
    .withConcurrentExecutionStrategy(ActionInstance.ConcurrentExecutionStrategy.ALLOW)
    .build();
```

#### Register the ActionInstance with ActionsOperator ####

Once you have a ```ActionInstance```, you can register it with the ```ActionsOperator```

```java
ActionsOperator actionsOperator = ActionsOperator.getInstance();
actionsOperator.registerActionInstance(actionInstance);
```
#### Execute the action ####

If you want to execute your ```Action``` apart from being executed by the ```Trigger``` (if ```ActionInstance``` is created with a trigger), then
you can use ```ActionsOperator``` to execute the ```ActionInstance``` as well

```java
actionsOperator.execute(actionInstance);
```
OR
```java
actionsOperator.execute(actionInstanceId);
```


#### Vew action execution history ####

```java
List<Execution> executions = actionsOperator.getExecutions(actionInstance.getId());
```

#### Cancel an action execution ####

```java
actionsOperator.cancel(execution);
```
OR
```java
actionsOperator.cancel(executionId);
```

Copyright and License
=

Copyright (C) 2015 Netflix. Licensed under the Apache License.

See [LICENSE.txt](https://github.com/spinnaker/scheduled-actions/blob/master/LICENSE.txt) for more information.
