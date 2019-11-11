/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.launch;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.eclipse.epsilon.common.util.OperatingSystem;
import org.eclipse.epsilon.evl.distributed.jms.EvlJmsWorker;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class JmsEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {
	
	protected final boolean localStandalone;
	
	@SuppressWarnings("unchecked")
	public static class Builder<R extends JmsEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		@Override
		protected EvlModuleJmsMaster createModule() {
			EvlContextJmsMaster context = new EvlContextJmsMaster(parallelism, distributedParallelism, getJobSplitter(), host, sessionID);
			return new EvlModuleJmsMaster(context);
		}
		
		public boolean localStandalone;
		
		public B withLocalStandalone() {
			localStandalone = true;
			return (B) this;
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}

	public JmsEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
		this.localStandalone = builder.localStandalone;
		getModule().setLogger(this::writeOut);
	}
	
	@Override
	public EvlModuleJmsMaster getModule() {
		return (EvlModuleJmsMaster) super.getModule();
	}
	
	@Override
	public void preExecute() throws Exception {
		if (localStandalone) {
			setupBroker();
			createWorkers();
		}
		super.preExecute();
	}

	protected void createWorkers() throws Exception {
		EvlContextJmsMaster context = getModule().getContext();
		final int numWorkers = context.getDistributedParallelism();
		final ArrayList<String> commands = new ArrayList<>(ManagementFactory.getRuntimeMXBean().getInputArguments());
		
		commands.add(0, "java");
		String jar = new File(EvlJmsWorker.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
		commands.add(1, "-jar");
		commands.add(2, jar);
		commands.add(this.basePath);
		commands.add(context.getSessionId() + "");
		commands.add(context.getBrokerHost());
		
		final String[] commandArr = commands.toArray(new String[commands.size()]);
		
		
		for (int i = 0; i < numWorkers; i++) {
			CompletableFuture.runAsync(() -> {
				try {
					OperatingSystem.executeCommand(commandArr);
				}
				catch (IOException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			});
		}
	}

	protected void setupBroker() throws Exception {
		//TODO implement
	}
}
