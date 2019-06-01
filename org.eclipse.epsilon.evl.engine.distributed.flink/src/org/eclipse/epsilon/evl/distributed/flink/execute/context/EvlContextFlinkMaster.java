/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.execute.context;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.configuration.Configuration;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextFlinkMaster extends EvlContextDistributedMaster {

	public EvlContextFlinkMaster(int distributedParallelism) {
		super(0, distributedParallelism);
	}
	
	public EvlContextFlinkMaster(EvlContextDistributedMaster other) {
		super(other);
	}

	public HashMap<String, Serializable> getJobParameters() {
		return getJobParameters(false);
	}
	
	public Configuration getJobConfiguration() {
		Configuration configuration = new Configuration();
		
		for (Map.Entry<String, ?> entry : getJobParameters().entrySet()) {
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
