package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.List;

/** Cleans up after any resources opened by {@link Task}. */
public class TaskFilter extends AbstractFilter {

    private final List<Task> tasks = new ArrayList<Task>();

    @Override
    protected void doInit() {
        for (Class<? extends RepeatingTask> taskClass : ObjectUtils.findClasses(RepeatingTask.class)) {
            try {
                RepeatingTask task = taskClass.newInstance();

                task.scheduleWithFixedDelay(1.0, 1.0);
                tasks.add(task);

            } catch (IllegalAccessException error) {
            } catch (InstantiationException error) {
            }
        }
    }

    @Override
    protected void doDestroy() {
        for (Task task : tasks) {
            task.stop();
        }

        tasks.clear();

        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            executor.shutdownNow();
        }
    }
}
