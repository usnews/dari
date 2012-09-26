package com.psddev.dari.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet for inspecting all active {@linkplain Task tasks}. */
@DebugFilter.Path("task")
@SuppressWarnings("serial")
public class TaskDebugServlet extends HttpServlet {

    // --- HttpServlet support ---

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        WebPageContext page = new WebPageContext(getServletContext(), request, response);
        if (page.isFormPost()) {
            String action = page.param(String.class, "action");
            String executorName = page.param(String.class, "executor");

            if (executorName != null) {
                TaskExecutor executor = TaskExecutor.Static.getInstance(executorName);
                if ("Pause All".equals(action)) {
                    executor.pauseTasks();
                } else if ("Resume All".equals(action)) {
                    executor.resumeTasks();
                } else if ("Stop All".equals(action)) {
                    executor.shutdown();
                }
            }

            page.redirect("");
            return;
        }

        new DebugFilter.PageWriter(page) {{
            startPage("Task Status");

            for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
                start("div", "style", "position: relative;");

                    start("h2").html(executor.getName()).end();

                    start("form", "method", "post", "style", "position: absolute; right: 0; top: 0;");
                        tag("input",
                                "name", "executor",
                                "type", "hidden",
                                "value", executor.getName());
                        tag("input",
                                "class", "btn btn-small btn-warning",
                                "name", "action",
                                "type", "submit",
                                "value", "Pause All");
                        html(' ');
                        tag("input",
                                "class", "btn btn-small btn-success",
                                "name", "action",
                                "type", "submit",
                                "value", "Resume All");
                        html(' ');
                        tag("input",
                                "class", "btn btn-small btn-danger",
                                "name", "action",
                                "type", "submit",
                                "value", "Stop All");
                    end();

                    List<Object> tasks = executor.getTasks();
                    if (tasks.isEmpty()) {
                        start("p", "class", "alert alert-info");
                            html("No tasks!");
                        end();

                    } else {
                        Map<AsyncQueue<?>, QueueTasks> queues = new LinkedHashMap<AsyncQueue<?>, QueueTasks>();

                        start("table", "class", "table table-condensed table-striped");
                            start("thead");
                                start("tr");
                                    start("th").html("Name").end();
                                    start("th", "colspan", 2).html("Progress").end();
                                    start("th").html("Last Exception").end();
                                    start("th").html("Run Count").end();
                                end();
                            end();

                            start("tbody");
                                for (Object taskObject : tasks) {
                                    if (!(taskObject instanceof Task)) {
                                        continue;
                                    }

                                    Task task = (Task) taskObject;
                                    start("tr");
                                        start("td").html(task.getName()).end();

                                        start("td");
                                            if (task.isPauseRequested()) {
                                                start("span", "class", "label label-warning");
                                                    html("Paused");
                                                end();

                                            } else if (task.isRunning()) {
                                                start("span", "class", "label label-success");
                                                    html("Running");
                                                end();

                                            } else {
                                                Future<?> future = task.getFuture();
                                                if (future instanceof ScheduledFuture) {
                                                    start("span", "class", "label label-warning");
                                                        html("Scheduled");
                                                    end();

                                                } else {
                                                    start("span", "class", "label label-important");
                                                        html("Stopped");
                                                    end();
                                                }
                                            }
                                        end();

                                        double runDuration = task.getRunDuration() / 1e3;
                                        if (runDuration < 0) {
                                            start("td");
                                            end();

                                        } else {
                                            start("td");
                                                String progress = task.getProgress();
                                                if (!ObjectUtils.isBlank(progress)) {
                                                    html(progress);
                                                    html("; ");
                                                }

                                                start("strong").object(runDuration).end();
                                                html(" s total");

                                                long index = task.getProgressIndex();
                                                if (index > 0) {
                                                    html("; ");
                                                    start("strong").object(index / runDuration).end();
                                                    html(" items/s");
                                                }

                                                if (task instanceof AsyncProducer) {
                                                    AsyncProducer<?> producer = (AsyncProducer<?>) task;
                                                    double count = producer.getProduceCount();
                                                    double produceDuration = producer.getProduceDuration() / 1e6;

                                                    html("; ");
                                                    start("strong").object(produceDuration / count).end();
                                                    html(" ms/produce");

                                                    AsyncQueue<?> queue = producer.getOutput();
                                                    getQueueTasks(queues, queue).producers.add(task);

                                                } else if (task instanceof AsyncConsumer) {
                                                    AsyncConsumer<?> consumer = (AsyncConsumer<?>) task;
                                                    double count = consumer.getConsumeCount();
                                                    double consumeDuration = consumer.getConsumeDuration() / 1e6;

                                                    html("; ");
                                                    start("strong").object(consumeDuration / count).end();
                                                    html(" ms/consume");

                                                    AsyncQueue<?> queue = consumer.getInput();
                                                    getQueueTasks(queues, queue).consumers.add(task);

                                                    if (task instanceof AsyncProcessor) {
                                                        queue = ((AsyncProcessor<?, ?>) task).getOutput();
                                                        getQueueTasks(queues, queue).producers.add(task);
                                                    }
                                                }
                                            end();
                                        }

                                        start("td").htmlOrDefault(task.getLastException(), "N/A").end();
                                        start("td").object(task.getRunCount()).end();
                                    end();
                                }
                            end();
                        end();

                        if (!queues.isEmpty()) {
                            start("h3").html(executor.getName()).html(" Queues").end();

                            start("table", "class", "table table-bordered table-condensed table-striped");
                                start("thead");
                                    start("tr");
                                        start("th", "colspan", 4).html("Production").end();
                                        start("th", "colspan", 3).html("Consumption").end();
                                    end();

                                    start("tr");
                                        start("th").html("Tasks").end();
                                        start("th").html("Successes").end();
                                        start("th").html("Failures").end();
                                        start("th").html("Wait").end();

                                        start("th").html("Tasks").end();
                                        start("th").html("Successes").end();
                                        start("th").html("Wait").end();
                                    end();
                                end();

                                start("tbody");
                                    for (Map.Entry<AsyncQueue<?>, QueueTasks> entry : queues.entrySet()) {
                                        AsyncQueue<?> queue = entry.getKey();
                                        QueueTasks queueTasks = entry.getValue();
                                        long addSuccessCount = queue.getAddSuccessCount();
                                        long addFailureCount = queue.getAddFailureCount();
                                        long removeCount = queue.getRemoveCount();

                                        start("tr");
                                            start("td").object(queueTasks.producers).end();
                                            start("td").object(addSuccessCount).end();
                                            start("td").object(addFailureCount).end();
                                            start("td").start("strong").object(((double) queue.getAddWait()) / ((double) (addSuccessCount + addFailureCount)) / 1e6).end().html(" ms/item").end();

                                            start("td").object(queueTasks.consumers).end();
                                            start("td").object(removeCount).end();
                                            start("td").start("strong").object(((double) queue.getRemoveWait()) / ((double) removeCount) / 1e6).end().html(" ms/item").end();
                                        end();
                                    }
                                end();
                            end();
                        end();
                    }
                }
            }

            endPage();
        }};
    }

    private static QueueTasks getQueueTasks(Map<AsyncQueue<?>, QueueTasks> queues, AsyncQueue<?> queue) {
        QueueTasks queueTasks = queues.get(queue);
        if (queueTasks == null) {
            queueTasks = new QueueTasks();
            queues.put(queue, queueTasks);
        }
        return queueTasks;
    }

    private static class QueueTasks {
        public final List<Task> producers = new ArrayList<Task>();
        public final List<Task> consumers = new ArrayList<Task>();
    }
}
