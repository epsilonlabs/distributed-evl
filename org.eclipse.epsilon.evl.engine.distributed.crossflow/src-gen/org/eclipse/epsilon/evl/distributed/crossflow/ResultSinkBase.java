package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;
import org.eclipse.scava.crossflow.runtime.Workflow;

public abstract class ResultSinkBase extends Task  implements ValidationOutputConsumer{
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "ResultSink:"+workflow.getName();
	}
	
	
	
	
	
	@Override
	public void consumeValidationOutputWithNotifications(ValidationResult validationResult) {
		
			try {
				workflow.setTaskInProgess(this);

				consumeValidationOutput(validationResult);

		


			} catch (Exception ex) {
				try {
					validationResult.setFailures(validationResult.getFailures()+1);
					workflow.getFailedJobsQueue().send(new FailedJob(validationResult, ex, workflow.getName(), "ResultSink"));
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

