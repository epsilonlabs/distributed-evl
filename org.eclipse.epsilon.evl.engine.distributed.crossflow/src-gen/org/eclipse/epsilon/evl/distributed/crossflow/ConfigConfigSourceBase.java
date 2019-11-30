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
public abstract class ConfigConfigSourceBase extends Task {

	/**
	 * Enum Identifier of this Task
	 */
	public static final DistributedEVLTasks TASK = DistributedEVLTasks.CONFIG_CONFIG_SOURCE; 
	
	protected DistributedEVL workflow;
	protected long timeout = 0;
	
	// Output Streams
	
	protected ConfigConfigTopic configConfigTopic;	
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

		
	protected void setConfigConfigTopic(ConfigConfigTopic configConfigTopic) {
		this.configConfigTopic = configConfigTopic;
	}

	protected ConfigConfigTopic getConfigConfigTopic() {
		return configConfigTopic;
	}

	public void sendToConfigConfigTopic(Config config) {
		config.setCacheable(this.cacheable);
		config.setTransactional(false);
		getConfigConfigTopic().send(config, TASK.getTaskName());
	}

	public abstract void produce() throws Exception;
	
}

