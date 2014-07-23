package com.sparkydots.mesos.example;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by renatb on 7/22/14.
 */
public class BasicExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(BasicExecutor.class);

    private static TaskStatus taskStatus(TaskInfo task, TaskState state) {
        TaskStatus status = TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(state).build();
        return status;
    }

    public static Protos.ExecutorInfo getExecutorInfo() throws IOException {
        Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
                .setCommand(Protos.CommandInfo.newBuilder().setValue("/usr/bin/java -Djava.library.path=/usr/local/lib -classpath /home/ubuntu/other/NamesOnMesos/build/libs/NamesOnMesos-1.0.jar com.sparkydots.mesos.example.BasicExecutorRunner 10.0.1.247:5050"))
                .setName("MyEXECUTOR")
                .setSource("java_test")
                .build();
        return executor;
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        logger.info("--registered:");
        logger.info(".  frameworkInfo: {}", frameworkInfo.getId().getValue());
        logger.info(".  slaveInfo: {}", slaveInfo.getId().getValue());
        logger.info(".  executorInfo.name: {}", executorInfo.getName());
        logger.info(".  executorInfo.command: {}", executorInfo.getCommand().getValue());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        logger.info(" -- reregistered --");
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        logger.info("-- disconnected --");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        logger.info("--  launch task callback ");
        logger.info(".  task.name: {} --", task.getName());
        try {
            logger.info(".  task.command: {} --", task.getData().toString("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        new Thread() {
            public void run() {
                try {
                    TaskStatus status = taskStatus(task, TaskState.TASK_RUNNING);
                    driver.sendStatusUpdate(status);

                    logger.info(" -*- running task here -*- ");
                    // This is where one would perform the requested task.

                    status = taskStatus(task, TaskState.TASK_FINISHED);
                    driver.sendStatusUpdate(status);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        logger.info("--  kill task callback ");
        logger.info(".  taskId: {} --", taskId.getValue());
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        logger.info("--  frameworkMessage: {} ", data);
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        logger.info("--  shutdown --");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        logger.info("--  error --");
    }
}
