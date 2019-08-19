/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * Base implementation of EVL with distributed execution semantics.
 * Splitting is supported at the element-level granularity. The {@link #checkConstraints()}
 * method initiates the distributed processing; which in turn should spawn instances of
 * {@link EvlModuleDistributedSlave}. If a data sink is used (i.e.the results can be
 * acquired by this module as they appear), the 
 * {@link SerializableEvlResultAtom#deserializeEager(org.eclipse.epsilon.evl.IEvlModule)} 
 * method can be used to rebuild the unsatisfied constraints and apply them to the context. Otherwise if
 * the processing is blocking (i.e. the master must wait for all results to become available), then
 * {@linkplain #assignDeserializedResults(Stream)} can be used.
 * 
 * @see {@link EvlModuleDistributedSlave}
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedMaster extends EvlModuleDistributed {
	
	protected JobSplitter<?, ?> jobSplitter;
	
	public EvlModuleDistributedMaster(EvlContextDistributedMaster context) {
		this(context, null);
	}
	
	protected EvlModuleDistributedMaster(EvlContextDistributedMaster context, JobSplitter<?, ?> strategy) {
		super(context);
		this.jobSplitter = strategy;
	}
	
	// UnsatisfiedConstraint resolution
	
	/**
	 * Resolves the serialized unsatisfied constraints lazily.
	 * 
	 * @param serializedResults The serialized UnsatisfiedConstraint instances.
	 * @return A Collection of lazily resolved UnsatisfiedConstraints.
	 */
	public Collection<LazyUnsatisfiedConstraint> deserializeLazy(Iterable<SerializableEvlResultAtom> serializedResults) {
		Collection<LazyUnsatisfiedConstraint> results = serializedResults instanceof Collection ?
			new ArrayList<>(((Collection<?>) serializedResults).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sr : serializedResults) {
			results.add(sr.deserializeLazy(this));
		}
		
		return results;
	}
	
	/**
	 * Deserializes the results eagerly in parallel using this context's ExecutorService.
	 * @param results The serialized results.
	 * @param eager Whether to fully resolve each UnsatisfiedConstraint.
	 * @return The deserialized UnsatisfiedConstraints.
	 * @throws EolRuntimeException
	 */
	public Collection<UnsatisfiedConstraint> deserializeEager(Iterable<? extends SerializableEvlResultAtom> results) throws EolRuntimeException {
		EvlContextDistributedMaster context = getContext();
		ArrayList<Callable<UnsatisfiedConstraint>> jobs = results instanceof Collection ?
			new ArrayList<>(((Collection<?>)results).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sera : results) {
			jobs.add(() -> sera.deserializeEager(this));
		}
		
		return context.executeParallelTyped(null, jobs);
	}
	
	/**
	 * Deserializes the object lazily if it is a valid result type and adds it to
	 * the unsatisfied constraints.
	 * 
	 * @param response The serializable result object.
	 * @return Whether the object was a valid result
	 * @throws EolRuntimeException
	 */
	@SuppressWarnings("unchecked")
	protected boolean deserializeResults(Object response) throws EolRuntimeException {
		if (response instanceof Iterable) {
			Iterable<SerializableEvlResultAtom> srIter;
			try {
				srIter = (Iterable<SerializableEvlResultAtom>) response;
			}
			catch (ClassCastException ccx) {
				return false;
			}
			getContext().getUnsatisfiedConstraints().addAll(deserializeLazy(srIter));
			return true;
		}
		else if (response instanceof Iterator) {
			java.util.function.Supplier<Iterator<Object>> iterSup = () -> (Iterator<Object>) response;
			return deserializeResults((Iterable<Object>) iterSup::get);
		}
		else if (response instanceof SerializableEvlResultAtom) {
			getContext().getUnsatisfiedConstraints().add(((SerializableEvlResultAtom) response).deserializeLazy(this));
			return true;
		}
		else if (response instanceof java.util.stream.BaseStream<?,?>) {
			return deserializeResults(((java.util.stream.BaseStream<?,?>) response).iterator());
		}
		else return false;
	}
	
	@Override
	protected void prepareContext() {
		getContext().storeInitialVariables();
		super.prepareContext();
	}
	
	@Override
	public EvlContextDistributedMaster getContext() {
		return (EvlContextDistributedMaster) super.getContext();
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextDistributedMaster) {
			super.setContext(context);
		}
		else if (context != null) {
			throw new IllegalArgumentException(
				"Invalid context type: expected "+EvlContextDistributedMaster.class.getName()
				+ " but got "+context.getClass().getName()
			);
		}
	}
}
