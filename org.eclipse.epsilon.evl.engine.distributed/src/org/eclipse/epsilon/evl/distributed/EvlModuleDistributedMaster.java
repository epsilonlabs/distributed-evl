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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

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

	public EvlModuleDistributedMaster(int distributedParallelism) {
		super(distributedParallelism);
		setContext(new EvlContextDistributedMaster(0, distributedParallelism));
	}
	
	// Job division

	protected abstract class JobSplitter<T, S extends Serializable> {
		protected final double masterProportion;
		protected final boolean shuffle;
		protected ArrayList<S> workerJobs;
		protected List<T> masterJobs;
		
		/**
		 * 
		 * @param masterProportion The percentage of jobs to be performed by the master. Must be between 0 and 1.
		 * @param shuffle Whether to randomise thr order of jobs.
		 * @throws IllegalArgumentException If the percentage is out of bounds.
		 */
		public JobSplitter(double masterProportion, boolean shuffle) {
			if ((this.masterProportion = masterProportion) > 1 || masterProportion < 0)
				throw new IllegalArgumentException("Percentage of master jobs must be a valid percentage");
			this.shuffle = shuffle;
		}
		
		public ArrayList<S> getWorkerJobs() throws EolRuntimeException {
			if (workerJobs == null) split();
			return workerJobs;
		}
		
		public List<T> getMasterJobs() throws EolRuntimeException {
			if (masterJobs == null) split();
			return masterJobs;
		}
		
		protected void split() throws EolRuntimeException {
			List<T> allJobs = getAllJobs();
			if (shuffle) Collections.shuffle(allJobs);
			
			int numTotalJobs = allJobs.size();
			int numMasterJobs = (int) (masterProportion * numTotalJobs);
			if (numMasterJobs >= numTotalJobs) {
				masterJobs = allJobs;
				workerJobs = new ArrayList<>(0);
			}
			else if (numMasterJobs <= 0) {
				masterJobs = null;
				Collection<S> wj = convertToWorkerJobs(allJobs);
				workerJobs = wj instanceof ArrayList ? (ArrayList<S>) wj : new ArrayList<>(wj);
			}
			else {
				masterJobs = allJobs.subList(0, numMasterJobs);
				Collection<S> wj = convertToWorkerJobs(allJobs.subList(numMasterJobs-1, numTotalJobs));
				workerJobs = wj instanceof ArrayList ? (ArrayList<S>) wj : new ArrayList<>(wj);
			}
		}
		
		protected abstract List<T> getAllJobs() throws EolRuntimeException;

		protected abstract Collection<S> convertToWorkerJobs(List<T> masterJobs) throws EolRuntimeException;
	}
	
	public class AtomicJobSplitter extends JobSplitter<ConstraintContextAtom, SerializableEvlInputAtom> {
		public AtomicJobSplitter(double masterProportion, boolean shuffle) {
			super(masterProportion, shuffle);
		}

		@Override
		protected List<ConstraintContextAtom> getAllJobs() throws EolRuntimeException {
			return getContextJobs();
		}
		
		@Override
		protected List<SerializableEvlInputAtom> convertToWorkerJobs(List<ConstraintContextAtom> masterJobs) throws EolRuntimeException {
			return SerializableEvlInputAtom.serializeJobs(masterJobs);
		}
	}

	public class BatchJobSplitter extends JobSplitter<DistributedEvlBatch, DistributedEvlBatch> {
		protected final double granularity;
		
		public BatchJobSplitter(double masterProportion, boolean shuffle, double granularity) {
			super(masterProportion, shuffle);
			if ((this.granularity = granularity) < 0 || granularity > 1)
				throw new IllegalArgumentException("Granularity must be a valid percentage");
		}
		
		@Override
		protected List<DistributedEvlBatch> convertToWorkerJobs(List<DistributedEvlBatch> masterJobs) throws EolRuntimeException {
			return masterJobs;
		}

		@Override
		protected List<DistributedEvlBatch> getAllJobs() throws EolRuntimeException {
			return getBatches(granularity / 2);
		}
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
	}
}
