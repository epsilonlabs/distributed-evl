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
public abstract class ProcessingBase extends Task  implements ValidationDataQueueConsumer,ConfigConfigTopicConsumer{

	/**
	 * Enum Identifier of this Task
	 */
	public static final DistributedEVLTasks TASK = DistributedEVLTasks.PROCESSING; 
	
	protected DistributedEVL workflow;
	protected long timeout = 0;
	
	// Output Streams
	
	protected ValidationOutput validationOutput;	
		
	// Output Sent Flags
	boolean hasSentToValidationOutput = false;
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

		
	protected void setValidationOutput(ValidationOutput validationOutput) {
		this.validationOutput = validationOutput;
	}

	protected ValidationOutput getValidationOutput() {
		return validationOutput;
	}

	public void sendToValidationOutput(ValidationResult validationResult) {
		validationResult.setCacheable(this.cacheable);
		hasSentToValidationOutput = true;
		getValidationOutput().send(validationResult, TASK.getTaskName());
	}

	public int getTotalOutputs() {
		return (hasSentToValidationOutput ? 1 : 0);
	}
	
	@Override
	public void consumeValidationDataQueueWithNotifications(ValidationData validationData) {
		// Await configuration to be processed
		while(!hasProcessedConfigConfigTopic) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				workflow.reportInternalException(e);
			}
		}
			
		try {
			workflow.getProcessings().getSemaphore().acquire();
		} catch (Exception e) {
			workflow.reportInternalException(e);
		}
		
		hasSentToValidationOutput = false;
		Runnable consumer = () -> {		
			try {
				workflow.setTaskInProgess(this);
		
				// Perform the actual processing
				consumeValidationDataQueue(validationData);
				
				// Send confirmation to ValidationOutput
				ValidationResult confirmationValidationOutput = new ValidationResult();
				confirmationValidationOutput.setCorrelationId(validationData.getJobId());
				confirmationValidationOutput.setIsTransactionSuccessMessage(true);
				confirmationValidationOutput.setTotalOutputs(getTotalOutputs());
				if (hasSentToValidationOutput) {
					sendToValidationOutput(confirmationValidationOutput);
				}
	
			} catch (Throwable ex) {
				try {
					boolean sendFailed = true;
					if (ex instanceof InterruptedException) {
						sendFailed = onConsumeValidationDataQueueTimeout(validationData);
					}
					if (sendFailed) {
						validationData.setFailures(validationData.getFailures()+1);
						workflow.getFailedJobsTopic().send(new FailedJob(validationData, new Exception(ex), this));
					}
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			} finally {
				try {
					workflow.getProcessings().getSemaphore().release();
					workflow.setTaskWaiting(this);
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			}
			
		};
		
		ListenableFuture<?> consumerFuture = workflow.getProcessings().getExecutor().submit(consumer);
		long timeout = validationData.getTimeout() > 0 ? validationData.getTimeout() : this.timeout;
		if (timeout > 0) {
			Futures.withTimeout(consumerFuture, validationData.getTimeout(), TimeUnit.SECONDS, workflow.getTimeoutManager());
		}
	}
	
	/**
	 * Cleanup callback in the event of a timeout.
	 *
	 * If this method returns {@code true} then a failed job will be registered by
	 * crossflow
	 *
	 * @param validationData original input
	 * @return {@code true} if a failed job should be registered, {@code false}
	 * otherwise.
	 */
	public boolean onConsumeValidationDataQueueTimeout(ValidationData validationData) throws Exception {
		return true;
	}
		
	public abstract void consumeValidationDataQueue(ValidationData validationData) throws Exception;
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

