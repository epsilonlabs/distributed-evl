/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;

@Generated(value = "org.eclipse.scava.crossflow.java.Task2BaseClass", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public abstract class ResultSinkBase extends Task  implements ValidationOutputConsumer{
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public DistributedEVL getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "ResultSink:" + workflow.getName();
	}
	
	
	
	
	
	@Override
	public void consumeValidationOutputWithNotifications(ValidationResult validationResult) {
		
			try {
				workflow.setTaskInProgess(this);

				consumeValidationOutput(validationResult);

		


			} catch (Exception ex) {
				try {
					validationResult.setFailures(validationResult.getFailures()+1);
					workflow.getFailedJobsTopic().send(new FailedJob(validationResult, ex, workflow.getName(), "ResultSink"));
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
	
	public abstract void consumeValidationOutput(ValidationResult validationResult) throws Exception;
	

	
	
}

