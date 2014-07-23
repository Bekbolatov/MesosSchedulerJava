package com.sparkydots.mesos.example;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Status;

/**
 * Created by renatb on 7/22/14.
 */
public class BasicExecutorRunner {
    public static void main(String[] args) {
        MesosExecutorDriver driver = new MesosExecutorDriver(new BasicExecutor());
        System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }
}
