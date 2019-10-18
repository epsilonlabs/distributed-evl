/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;

@Generated(value = "org.eclipse.scava.crossflow.java.Task2BaseClass", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public abstract class ConfigConfigSourceBase extends Task {
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public DistributedEVL getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "ConfigConfigSource:" + workflow.getName();
	}
	
	protected ConfigConfigTopic configConfigTopic;
	
	protected void setConfigConfigTopic(ConfigConfigTopic configConfigTopic) {
		this.configConfigTopic = configConfigTopic;
	}
	
	protected ConfigConfigTopic getConfigConfigTopic() {
		return configConfigTopic;
	}
	
	public void sendToConfigConfigTopic(Config config) {
		config.setCacheable(this.cacheable);
		config.setTransactional(false);
		getConfigConfigTopic().send(config, this.getClass().getName());
	}
	
	
	
	public abstract void produce() throws Exception;
	
}

