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
public abstract class ResultSinkBase extends Task  implements ValidationOutputConsumer{

	/**
	 * Enum Identifier of this Task
	 */
	public static final DistributedEVLTasks TASK = DistributedEVLTasks.RESULT_SINK; 
	
	protected DistributedEVL workflow;
	protected long timeout = 0;
	
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

	@Override
	public void consumeValidationOutputWithNotifications(ValidationResult validationResult) {
			try {
				workflow.setTaskInProgess(this);
		
				// Perform the actual processing
				consumeValidationOutput(validationResult);
				
				
	
			} catch (Throwable ex) {
				try {
					boolean sendFailed = true;
					if (ex instanceof InterruptedException) {
						sendFailed = onConsumeValidationOutputTimeout(validationResult);
					}
					if (sendFailed) {
						validationResult.setFailures(validationResult.getFailures()+1);
						workflow.getFailedJobsTopic().send(new FailedJob(validationResult, new Exception(ex), this));
					}
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			} finally {
				try {
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
	 * @param validationResult original input
	 * @return {@code true} if a failed job should be registered, {@code false}
	 * otherwise.
	 */
	public boolean onConsumeValidationOutputTimeout(ValidationResult validationResult) throws Exception {
		return true;
	}
		
	public abstract void consumeValidationOutput(ValidationResult validationResult) throws Exception;
}

