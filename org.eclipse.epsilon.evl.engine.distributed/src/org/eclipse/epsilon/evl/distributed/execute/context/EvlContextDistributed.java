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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributed extends EvlContextParallel {
	
	protected static final String
		ENCODING = java.nio.charset.StandardCharsets.UTF_8.toString(),
		BASE_PATH = "basePath",
		LOCAL_PARALLELISM = "localParallelism",
		DISTRIBUTED_PARALLELISM = "distributedParallelism",
		EVL_SCRIPT = "evlScript",
		OUTPUT_DIR = "output",
		NUM_MODELS = "numberOfModels",
		MODEL_PREFIX = "model",
		SCRIPT_PARAMS = "scriptParameters",
		IGNORE_MODELS = "noModelLoading";
	
	public static final String
		BASE_PATH_SUBSTITUTE = "$BASEPATH$",
		BASE_PATH_SYSTEM_PROPERTY = "org.eclipse.epsilon.evl.distributed."+BASE_PATH;
	
	public EvlContextDistributed(IEvlContext other) {
		super(other);
	}

	public EvlContextDistributed() {
		super();
	}
	
	public EvlContextDistributed(int localParallelism) {
		super(localParallelism);
	}

	public void setUnsatisfiedConstraints(Set<UnsatisfiedConstraint> unsatisfiedConstraints) {
		this.unsatisfiedConstraints = unsatisfiedConstraints;
	}
	
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		if (job == null) {
			return null;
		}
		else if (job instanceof SerializableEvlResultAtom) {
			return Collections.singletonList((SerializableEvlResultAtom) job);
		}
		else if (job instanceof SerializableEvlInputAtom) {
			((SerializableEvlInputAtom) job).execute(getModule());
			return null;
		}
		else {
			return super.executeJob(job);
		}
	}
	
	/**
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * The context's state (i.e. the UnsatisfiedConstraints) is not modified.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s,
	 * or <code>null</code> if this module is the master.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	public final Collection<SerializableEvlResultAtom> executeJobStateless(Object job) throws EolRuntimeException {
		final Set<UnsatisfiedConstraint>
			originalUc = getUnsatisfiedConstraints(),
			tempUc = ConcurrencyUtils.concurrentSet(16, getParallelism());
		setUnsatisfiedConstraints(tempUc);
		
		try {
			executeJob(job);
			return serializeResults(tempUc);
		}
		finally {
			originalUc.addAll(tempUc);
			setUnsatisfiedConstraints(originalUc);
		}
	}
	
	protected Collection<SerializableEvlResultAtom> serializeResults(Collection<UnsatisfiedConstraint> unsatisfiedConstraints) {
		return unsatisfiedConstraints.parallelStream()
			.map(uc -> SerializableEvlResultAtom.serializeResult(uc, this))
			.collect(Collectors.toCollection(ArrayList::new));
	}
}
