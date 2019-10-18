/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;

@Generated(value = "org.eclipse.scava.crossflow.java.Task2BaseClass", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public abstract class JobDistributorBase extends Task  implements ConfigConfigTopicConsumer{
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public DistributedEVL getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "JobDistributor:" + workflow.getName();
	}
	
	protected ValidationDataQueue validationDataQueue;
	
	protected void setValidationDataQueue(ValidationDataQueue validationDataQueue) {
		this.validationDataQueue = validationDataQueue;
	}
	
	protected ValidationDataQueue getValidationDataQueue() {
		return validationDataQueue;
	}
	
	public void sendToValidationDataQueue(ValidationData validationData) {
		validationData.setCacheable(this.cacheable);
		getValidationDataQueue().send(validationData, this.getClass().getName());
	}
	
	
	
	boolean hasProcessedConfigConfigTopic = false;
	
	
	@Override
	public final void consumeConfigConfigTopicWithNotifications(Config config) {
		
			try {
				workflow.setTaskInProgess(this);

				consumeConfigConfigTopic(config);

			} catch (Exception ex) {
				try {
					config.setFailures(config.getFailures()+1);
					workflow.getFailedJobsTopic().send(new FailedJob(config, ex, workflow.getName(), "JobDistributor"));
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

