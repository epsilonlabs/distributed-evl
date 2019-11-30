/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import java.util.concurrent.TimeUnit;

import javax.annotation.Generated;

import org.eclipse.scava.crossflow.runtime.BuiltinStream;
import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

@Generated(value = "org.eclipse.scava.crossflow.java.Task2BaseClass", date = "2019-11-30T17:04:27.022703400Z")
public abstract class JobDistributorBase extends Task  implements ConfigConfigTopicConsumer{

	/**
	 * Enum Identifier of this Task
	 */
	public static final DistributedEVLTasks TASK = DistributedEVLTasks.JOB_DISTRIBUTOR; 
	
	protected DistributedEVL workflow;
	protected long timeout = 0;
	
	// Output Streams
	
	protected ValidationDataQueue validationDataQueue;	
	// Configuration Received Flags
	boolean hasProcessedConfigConfigTopic = false;

	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}

	public DistributedEVL getWorkflow() {
		return workflow;
	}
	
	@Override
	public String getName() {
		return TASK.getTaskName();
	}

	public DistributedEVLTasks getTaskEnum() {
		return TASK;
	}
	
	public long getTimeout() {
		return timeout;
	}
	
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

		
	protected void setValidationDataQueue(ValidationDataQueue validationDataQueue) {
		this.validationDataQueue = validationDataQueue;
	}

	protected ValidationDataQueue getValidationDataQueue() {
		return validationDataQueue;
	}

	public void sendToValidationDataQueue(ValidationData validationData) {
		validationData.setCacheable(this.cacheable);
		getValidationDataQueue().send(validationData, TASK.getTaskName());
	}

	@Override
	public void consumeConfigConfigTopicWithNotifications(Config config) {
			try {
				workflow.setTaskInProgess(this);
		
				// Perform the actual processing
				consumeConfigConfigTopic(config);
				
	
			} catch (Throwable ex) {
				try {
					boolean sendFailed = true;
					if (ex instanceof InterruptedException) {
						sendFailed = onConsumeConfigConfigTopicTimeout(config);
					}
					if (sendFailed) {
						config.setFailures(config.getFailures()+1);
						workflow.getFailedJobsTopic().send(new FailedJob(config, new Exception(ex), this));
					}
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			} finally {
				try {
					hasProcessedConfigConfigTopic = true;
					workflow.setTaskWaiting(this);
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			}
	}
	
	/**
	 * Cleanup callback in the event of a timeout.
	 *
	 * If this method returns {@code true} then a failed job will be registered by
	 * crossflow
	 *
	 * @param config original input
	 * @return {@code true} if a failed job should be registered, {@code false}
	 * otherwise.
	 */
	public boolean onConsumeConfigConfigTopicTimeout(Config config) throws Exception {
		return true;
	}
		
	public abstract void consumeConfigConfigTopic(Config config) throws Exception;
}

