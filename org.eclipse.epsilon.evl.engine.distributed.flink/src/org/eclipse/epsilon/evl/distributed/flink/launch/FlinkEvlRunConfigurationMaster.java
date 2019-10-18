/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.launch;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.configuration.Configuration;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class FlinkEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {
	
	public static class Builder<R extends FlinkEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		@Override
		protected EvlModuleFlinkMaster createModule() {
			EvlContextFlinkMaster context = new EvlContextFlinkMaster(parallelism, distributedParallelism, getJobSplitter(), outputFile);
			return new EvlModuleFlinkMaster(context);
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	public static Builder<FlinkEvlRunConfigurationMaster, ?> Builder() {
		return new Builder<>(FlinkEvlRunConfigurationMaster.class);
	}
	
	public FlinkEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected Configuration getJobParameters(boolean stripBasePath) {
		return getJobConfiguration((Map<String, ?>) super.getJobParameters(stripBasePath));
	}
	
	public Configuration getJobConfiguration(final Map<String, ?> configParams) {
		Configuration configuration = new Configuration();
		
		for (Map.Entry<String, ?> entry : configParams.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			
			if (value instanceof Boolean) {
				configuration.setBoolean(key, (boolean) value);
			}
			else if (value instanceof Integer) {
				configuration.setInteger(key, (int) value);
			}
			else if (value instanceof Long) {
				configuration.setLong(key, (long) value);
			}
			else if (value instanceof Float) {
				configuration.setFloat(key, (float) value);
			}
			else if (value instanceof Double) {
				configuration.setDouble(key, (double) value);
			}
			else {
				configuration.setString(key, Objects.toString(value));
			}
		}
		
		return configuration;
	}
}
