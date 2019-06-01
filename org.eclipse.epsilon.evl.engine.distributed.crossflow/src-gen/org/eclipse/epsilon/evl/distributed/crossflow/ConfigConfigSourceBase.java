package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;
import org.eclipse.scava.crossflow.runtime.Workflow;

public abstract class ConfigConfigSourceBase extends Task {
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "ConfigConfigSource:"+workflow.getName();
	}
	
	protected ConfigTopic configTopic;
	
	protected void setConfigTopic(ConfigTopic configTopic) {
		this.configTopic = configTopic;
	}
	
	protected ConfigTopic getConfigTopic() {
		return configTopic;
	}
	
	public void sendToConfigTopic(Config config) {
		config.setCacheable(this.cacheable);
		config.setTransactional(false);
		getConfigTopic().send(config, this.getClass().getName());
	}
	
	
	
	public abstract void produce() throws Exception;
	
}

