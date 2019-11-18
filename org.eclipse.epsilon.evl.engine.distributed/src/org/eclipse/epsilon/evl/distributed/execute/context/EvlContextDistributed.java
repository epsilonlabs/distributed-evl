/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.context;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.control.ExecutionController;
import org.eclipse.epsilon.eol.execute.control.ExecutionProfiler;
import org.eclipse.epsilon.erl.dom.*;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 *
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlContextDistributed extends EvlContextParallel {

	public EvlContextDistributed() {
		super();
	}
	
	public EvlContextDistributed(int localParallelism) {
		super(localParallelism);
	}
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		if (job instanceof LazyUnsatisfiedConstraint) {
			return ((LazyUnsatisfiedConstraint) job).getProxy();
		}
		if (job instanceof SerializableEvlResult) {
			return ((SerializableEvlResult) job).deserializeLazy(getModule());
		}
		if (job instanceof SerializableEvlInputAtom) {
			((SerializableEvlInputAtom) job).execute(getModule());
			return null;
		}
		return super.executeJob(job);
	}
	
	/**
	 * Convenience method for serializing the profiling information of a
	 * slave worker to be sent to the master.
	 * 
	 * @return A serializable representation of {@link ExecutionProfiler} if
	 * profiling is enabled, <code>null</code> otherwise.
	 */
	public HashMap<String, java.time.Duration> getSerializableRuleExecutionTimes() {
		ExecutionController controller = getExecutorFactory().getExecutionController();
		if (controller instanceof ExecutionProfiler) {
			return ((ExecutionProfiler) controller)
				.getExecutionTimes()
				.entrySet().stream()
				.filter(entry -> !(entry.getKey() instanceof Pre || entry.getKey() instanceof Post))
				.collect(Collectors.toMap(
					e -> e.getKey().toString(),
					Map.Entry::getValue,
					(t1, t2) -> t1.plus(t2),
					HashMap::new
				));
		}
		return null;
	}
	
	@Override
	public EvlModuleDistributed getModule() {
		return (EvlModuleDistributed) super.getModule();
	}
}
