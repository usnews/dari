package com.psddev.dari.util;

/** Cleans up after any resources opened by {@link Task}. */
public class TaskFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doDestroy() {
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            executor.shutdownNow();
        }
    }
}
