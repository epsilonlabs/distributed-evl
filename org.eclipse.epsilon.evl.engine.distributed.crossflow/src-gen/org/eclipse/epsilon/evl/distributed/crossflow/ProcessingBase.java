package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;
import org.eclipse.scava.crossflow.runtime.Workflow;

public abstract class ProcessingBase extends Task  implements ValidationDataQueueConsumer,ConfigTopicConsumer{
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "Processing:"+workflow.getName();
	}
	
	protected ValidationOutput validationOutput;
	
	protected void setValidationOutput(ValidationOutput validationOutput) {
		this.validationOutput = validationOutput;
	}
	
	protected ValidationOutput getValidationOutput() {
		return validationOutput;
	}
	
	public void sendToValidationOutput(ValidationResult validationResult) {
		validationResult.setCacheable(this.cacheable);
		hasSentToValidationOutput = true;
		getValidationOutput().send(validationResult, this.getClass().getName());
	}
	
	boolean hasSentToValidationOutput = false;
	
	
	
	
	@Override
	public final void consumeValidationDataQueueWithNotifications(ValidationData validationData) {
		
		while(!hasProcessedConfigTopic)
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			workflow.reportInternalException(e);
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

				consumeValidationDataQueue(validationData);

				ValidationResult conf = new ValidationResult();
				conf.setCorrelationId(validationData.getId());
				conf.setIsTransactionSuccessMessage(true);
				conf.setTotalOutputs((hasSentToValidationOutput ? 1 : 0));
				if (hasSentToValidationOutput) {
					sendToValidationOutput(conf);
				}
		


			} catch (Exception ex) {
				try {
					validationData.setFailures(validationData.getFailures()+1);
					workflow.getFailedJobsQueue().send(new FailedJob(validationData, ex, workflow.getName(), "Processing"));
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

		workflow.getProcessings().getExecutor().submit(consumer);
	}
	
	public abstract void consumeValidationDataQueue(ValidationData validationData) throws Exception;
	

	
	boolean hasProcessedConfigTopic = false;
	
	
	@Override
	public final void consumeConfigTopicWithNotifications(Config config) {
		
			try {
				workflow.setTaskInProgess(this);

				consumeConfigTopic(config);

			} catch (Exception ex) {
				try {
					config.setFailures(config.getFailures()+1);
					workflow.getFailedJobsQueue().send(new FailedJob(config, ex, workflow.getName(), "Processing"));
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			} finally {
				try {
					hasProcessedConfigTopic = true;
					workflow.setTaskWaiting(this);
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			}
	}
	
	public abstract void consumeConfigTopic(Config config) throws Exception;
	

	
	
}

