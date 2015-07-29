package com.netflix.scheduledactions;

import java.util.List;

/**
 * Any class that implements the {@code Action} interface can be executed as a {@code SimpleTrigger}
 */
public interface Action {
    /**
     * An action can override the current event status and return whatever status it wants to
     * @return
     */
    public Status getStatus();

    /**
     * An action can override the trigger owners.
     * For example, for a given deployment action, the list of committers could be the owners of the action
     * @return
     */
    public List<String> getOwners();

    /**
     * An action can override the trigger watchers
     * For example, for a given deployment action, the list of designated on-calls could be the watchers of the action
     * @return
     */
    public List<String> getWatchers();

    /**
     * Executes the action
     * @param context
     * @throws Exception
     */
    public void execute(Context context, Execution execution) throws Exception;

}
