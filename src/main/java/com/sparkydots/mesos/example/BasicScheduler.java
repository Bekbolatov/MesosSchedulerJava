package com.sparkydots.mesos.example;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class BasicScheduler implements Scheduler {
    private static Logger logger = LoggerFactory.getLogger(BasicScheduler.class);

    private final ExecutorInfo executorInfo;
    private int num;

    public BasicScheduler(ExecutorInfo executorInfo, int num) {
        this.executorInfo = executorInfo;
        this.num = num;
    }

    /**
     * Invoked when resources have been offered to this framework. A
     * single offer will only contain resources from a single slave.
     * Resources associated with an offer will not be re-offered to
     * _this_ framework until either (a) this framework has rejected
     * those resources (see {@link org.apache.mesos.SchedulerDriver#launchTasks}) or (b)
     * those resources have been rescinded (see {@link #offerRescinded}).
     * Note that resources may be concurrently offered to more than one
     * framework at a time (depending on the allocator being used). In
     * that case, the first framework to launch tasks using those
     * resources will be able to use them while the other frameworks
     * will have those resources rescinded (or if a framework has
     * already launched tasks with those resources then those tasks will
     * fail with a TASK_LOST status and a message saying as much).
     *
     * @param driver The driver that was used to run this scheduler.
     * @param offers The resources offered to this framework.
     */
    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        logger.info("-- resourceOffers: {}", offers.size());

        if (offers.size() < 1 || num < 1) {
            return;
        }

        Offer offer = offers.get(0);

        TaskID taskId = TaskID.newBuilder().setValue("ID32" + num).build();

        TaskInfo task = null;
        try {
            task = TaskInfo.newBuilder()
                    .setName("task-" + taskId.getValue())
                    .setTaskId(taskId)
                    .setData(ByteString.copyFrom("datapiece*" + num, "UTF-8"))
                    .setSlaveId(offer.getSlaveId())
                    .addResources(Resource.newBuilder()
                            .setName("cpus")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder().setValue(1)))
                    .addResources(Resource.newBuilder()
                            .setName("mem")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder().setValue(128)))
                    .setExecutor(ExecutorInfo.newBuilder(executorInfo))
                    .build();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();

        driver.launchTasks(Lists.newArrayList(offer.getId()), Lists.newArrayList(task), filters);
        num--;

    }

    /**
     * Invoked when an offer is no longer valid (e.g., the slave was
     * lost or another framework used resources in the offer). If for
     * whatever reason an offer is never rescinded (e.g., dropped
     * message, failing over framework, etc.), a framwork that attempts
     * to launch tasks using an invalid offer will receive TASK_LOST
     * status updats for those tasks (see {@link #resourceOffers}).
     *
     * @param driver  The driver that was used to run this scheduler.
     * @param offerId The ID of the offer that was rescinded.
     */
    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        logger.info("-- offerRescinded --");
    }

    /**
     * Invoked when the status of a task has changed (e.g., a slave is
     * lost and so the task is lost, a task finishes and an executor
     * sends a status update saying so, etc). Note that returning from
     * this callback _acknowledges_ receipt of this status update! If
     * for whatever reason the scheduler aborts during this callback (or
     * the process exits) another status update will be delivered (note,
     * however, that this is currently not true if the slave sending the
     * status update is lost/fails during that time).
     *
     * @param driver The driver that was used to run this scheduler.
     * @param status The status update, which includes the task ID and status.
     */
    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        logger.info("-- statusUpdate from {}: {} --", status.getSlaveId().getValue(), status.getMessage());
    }

    /**
     * Invoked when an executor sends a message. These messages are best
     * effort; do not expect a framework message to be retransmitted in
     * any reliable fashion.
     *
     * @param driver     The driver that was used to run this scheduler.
     * @param executorId The ID of the executor that sent the message.
     * @param slaveId    The ID of the slave that launched the executor.
     * @param data       The message payload.
     */
    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        logger.info("-- frameworkMessage: {} --", data.toString());

    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        logger.info("-- registered frameworkID: {}, master: {} --", frameworkId.getValue(), masterInfo.getHostname());
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo newMasterInfo) {
        logger.info("-- reregistered with new master: {} --", newMasterInfo.getHostname());
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        logger.info("-- disconnected --");
    }

    /**
     * Invoked when a slave has been determined unreachable (e.g.,
     * machine failure, network partition). Most frameworks will need to
     * reschedule any tasks launched on this slave on a new slave.
     *
     * @param driver  The driver that was used to run this scheduler.
     * @param slaveId The ID of the slave that was lost.
     */
    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        logger.info("-- slaveLost --");
    }

    /**
     * Invoked when an executor has exited/terminated. Note that any
     * tasks running will have TASK_LOST status updates automagically
     * generated.
     *
     * @param driver     The driver that was used to run this scheduler.
     * @param executorId The ID of the executor that was lost.
     * @param slaveId    The ID of the slave that launched the executor.
     * @param status     The exit status of the executor.
     */
    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        logger.info("-- executorLost --");
    }

    /**
     * Invoked when there is an unrecoverable error in the scheduler or
     * driver. The driver will be aborted BEFORE invoking this callback.
     *
     * @param driver  The driver that was used to run this scheduler.
     * @param message The error message.
     */
    @Override
    public void error(SchedulerDriver driver, String message) {
        logger.info("-- error: {} --", message);
    }
}
