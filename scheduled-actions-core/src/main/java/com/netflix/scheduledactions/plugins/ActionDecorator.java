package com.netflix.scheduledactions.plugins;

import com.netflix.scheduledactions.Action;

import java.util.Map;

/**
 * Users can register a {@code ActionDecorator} with the {@code SimpleTrigger} to decorate
 * their action instances
 * @author sthadeshwar
 */
public interface ActionDecorator {
    public Action decorate(Action action, Map<String, Object> params);
}
