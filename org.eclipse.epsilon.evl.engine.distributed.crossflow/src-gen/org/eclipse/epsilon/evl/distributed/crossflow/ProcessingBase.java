/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;

@Generated(value = "org.eclipse.scava.crossflow.java.Task2BaseClass", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public abstract class ProcessingBase extends Task  implements ValidationDataQueueConsumer,ConfigConfigTopicConsumer{
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public DistributedEVL getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "Processing:" + workflow.getName();
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
		
		while(!hasProcessedConfigConfigTopic)
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

		
		
				ValidationResult confirmationValidationOutput = new ValidationResult();
				confirmationValidationOutput.setCorrelationId(validationData.getId());
				confirmationValidationOutput.setIsTransactionSuccessMessage(true);
				confirmationValidationOutput.setTotalOutputs((hasSentToValidationOutput ? 1 : 0));
				if (hasSentToValidationOutput) {
					sendToValidationOutput(confirmationValidationOutput);
				}
			
		


			} catch (Exception ex) {
				try {
					validationData.setFailures(validationData.getFailures()+1);
					workflow.getFailedJobsTopic().send(new FailedJob(validationData, ex, workflow.getName(), "Processing"));
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
	

	
	boolean hasProcessedConfigConfigTopic = false;
	
	
	@Override
	public final void consumeConfigConfigTopicWithNotifications(Config config) {
		
			try {
				workflow.setTaskInProgess(this);

				consumeConfigConfigTopic(config);

			} catch (Exception ex) {
				try {
					config.setFailures(config.getFailures()+1);
					workflow.getFailedJobsTopic().send(new FailedJob(config, ex, workflow.getName(), "Processing"));
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
	
	public abstract void consumeConfigConfigTopic(Config config) throws Exception;
	

	
	
}

