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
import java.util.Set;
import java.util.concurrent.Callable;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
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
		if (job instanceof SerializableEvlResultAtom) {
			return ((SerializableEvlResultAtom) job).deserializeLazy(getModule());
		}
		if (job instanceof SerializableEvlResultPointer) {
			return ((SerializableEvlResultPointer) job).resolveForModule(getModule());
		}
		if (job instanceof UnsatisfiedConstraint) {
			return SerializableEvlResultAtom.serializeResult((UnsatisfiedConstraint) job, this);
		}
		if (job instanceof SerializableEvlInputAtom) {
			((SerializableEvlInputAtom) job).execute(getModule());
			return null;
		}
		return super.executeJob(job);
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
			setUnsatisfiedConstraints(originalUc);
		}
	}
	
	/**
	 * Transforms the {@linkplain UnsatisfiedConstraint}s into their Serializable form.
	 * 
	 * @param unsatisfiedConstraints The UnsatisfiedConstraints
	 * @return A Serializable collection representing the UnsatisfiedConstraints.
	 * @throws EolRuntimeException If anything goes wrong during the conversion to a serialized form.
	 */
	protected Collection<SerializableEvlResultAtom> serializeResults(Collection<UnsatisfiedConstraint> unsatisfiedConstraints) throws EolRuntimeException {
		ArrayList<SerializableEvlResultAtom> results = new ArrayList<>(unsatisfiedConstraints.size());
		for (UnsatisfiedConstraint uc : unsatisfiedConstraints) {
			results.add(SerializableEvlResultAtom.serializeResult(uc, this));
		}
		return results;
	}
	
	
	/**
	 * Resolves the serialized unsatisfied constraints lazily.
	 * 
	 * @param serializedResults The serialized UnsatisfiedConstraint instances.
	 * @return A Collection of lazily resolved UnsatisfiedConstraints.
	 */
	public Collection<LazyUnsatisfiedConstraint> deserializeLazy(Collection<SerializableEvlResultAtom> serializedResults) {
		IEvlModule module = getModule();
		Collection<LazyUnsatisfiedConstraint> results = new ArrayList<>(serializedResults.size());		
		for (SerializableEvlResultAtom sr : serializedResults) {
			results.add(sr.deserializeLazy(module));
		}
		return results;
	}
	
	/**
	 * Deserializes the results eagerly in parallel using this context's ExecutorService.
	 * 
	 * @param results The serialized results.
	 * @param eager Whether to fully resolve each UnsatisfiedConstraint.
	 * @return The deserialized UnsatisfiedConstraints.
	 * @throws EolRuntimeException
	 */
	public Collection<UnsatisfiedConstraint> deserializeEager(Collection<? extends SerializableEvlResultAtom> results) throws EolRuntimeException {
		IEvlModule module = getModule();
		ArrayList<Callable<UnsatisfiedConstraint>> jobs = new ArrayList<>(results.size());	
		for (SerializableEvlResultAtom sera : results) {
			jobs.add(() -> sera.deserializeEager(module));
		}
		return executeParallelTyped(null, jobs);
	}
	
	@Override
	public EvlModuleDistributed getModule() {
		return (EvlModuleDistributed) super.getModule();
	}
	
	@Override
	public void setModule(IModule module) {
		if (module instanceof EvlModuleDistributed) {
			super.setModule(module);
		}
	}
}
