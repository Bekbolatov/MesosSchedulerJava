package com.sparkydots.mesos.example;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;

import java.io.IOException;

/**
 * Created by renatb on 7/22/14.
 */
public class BasicSchedulerRunner {
    public static void main(String[] args) throws IOException {
        String master = getMasterIp(args);

        ExecutorInfo executorInfo = BasicExecutor.getExecutorInfo();

        Scheduler scheduler = new BasicScheduler(executorInfo, 3);
        FrameworkInfo framework = getFrameworkInfo("", "Framework1");
        MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(scheduler, framework, master);

        int status = schedulerDriver.run() == Status.DRIVER_STOPPED ? 0 : 1;
        schedulerDriver.stop();

        System.exit(status);
    }

    private static FrameworkInfo getFrameworkInfo(String userName, String frameworkName) {
        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
                .setUser(userName).setName(frameworkName);
        FrameworkInfo framework = frameworkBuilder.build();
        return framework;
    }


    private static String getMasterIp(String[] args) {
        if (args.length > 0) {
            return args[0];
        }
        return "54.91.235.111:5050";
    }

}
