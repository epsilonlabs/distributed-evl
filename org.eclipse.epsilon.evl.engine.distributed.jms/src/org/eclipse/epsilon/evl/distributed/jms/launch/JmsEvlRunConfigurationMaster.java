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
import org.eclipse.epsilon.common.util.OperatingSystem;
import org.eclipse.epsilon.evl.distributed.jms.EvlJmsWorker;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class JmsEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {
	
	public static class Builder<R extends JmsEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		@Override
		protected EvlModuleJmsMaster createModule() {
			EvlContextJmsMaster context = new EvlContextJmsMaster(parallelism, distributedParallelism, getJobSplitter(), host, sessionID);
			return new EvlModuleJmsMaster(context);
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}

	public JmsEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfigurationMaster, ?> builder) {
		super(builder);
		getModule().setLogger(this::writeOut);
	}
	
	public static Builder<? extends JmsEvlRunConfigurationMaster, ?> Builder() {
		return new Builder<>(JmsEvlRunConfigurationMaster.class);
	}
	
	@Override
	public EvlModuleJmsMaster getModule() {
		return (EvlModuleJmsMaster) super.getModule();
	}

	@Override
	protected void createStandaloneWorkers() throws Exception {
		EvlContextJmsMaster context = getModule().getContext();
		final int numWorkers = context.getDistributedParallelism();
		final ArrayList<String> commands = new ArrayList<>(ManagementFactory.getRuntimeMXBean().getInputArguments());
		
		int index = 0;
		commands.add(index++, "java");
		String src = new File(EvlJmsWorker.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
		if (src.endsWith(".jar")) {
			commands.add(index++, "-jar");
			commands.add(index++, '"'+src+'"');
		}
		else {
			commands.add(index++, "-cp");
			commands.add(index++, '"'+System.getProperty("java.class.path")+'"');
			commands.add(index++, EvlJmsWorker.class.getName());
		}
		commands.add(index++, '"'+this.basePath+'"');
		commands.add(index++, context.getSessionId() + "");
		commands.add(index++, context.getBrokerHost());
		
		final String[] commandArr = commands.toArray(new String[commands.size()]);
		
		for (int i = 0; i < numWorkers; i++) {
			new Thread(() -> {
				try {
					OperatingSystem.executeCommand(commandArr);
				}
				catch (IOException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			}).start();
		}
	}
}
