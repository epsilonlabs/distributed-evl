/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.strategy;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.execute.atoms.EvlAtom;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class JobSplitter {

	protected final boolean shuffleJobs;
	protected double masterProportion, batchSize;
	
	protected EvlContextDistributedMaster context;
	protected Collection<? extends Serializable> workerJobs;
	protected Collection<?> masterJobs;
	
	public void setContext(EvlContextDistributedMaster context) {
		this.context = context;
	}
	
	public JobSplitter(boolean randomJobOrder, double masterProportion, double batchFactor) {
		this.shuffleJobs = randomJobOrder;
		this.masterProportion = masterProportion;
		this.batchSize = batchFactor;
	}
	
	public List<JobBatch> getBatchJobs(List<?> jobList) throws EolRuntimeException {
		final int numTotalJobs = jobList.size(), chunks;
		
		if (batchSize < 0) {
			batchSize = context.getParallelism();
		}
		else if (batchSize == 0) {
			batchSize = numTotalJobs;
		}
		
		if (batchSize >= 1) {
			chunks = (int) batchSize;
		}
		else {
			assert batchSize < 1 && batchSize > 0;
			final int adjusted = Math.max(context.getDistributedParallelism(), 1);
			chunks = (int) (numTotalJobs * ((1-batchSize) / adjusted));
		}
		return JobBatch.getBatches(numTotalJobs, chunks);
	}

	public boolean isBatchBased() {
		return batchSize > -1;
	}
	
	protected void requireSplit() throws IllegalArgumentException {
		if (masterJobs == null || workerJobs == null) {
			throw new IllegalStateException("Must call split() first!");
		}
	}
	
	public Collection<?> getMasterJobs() throws EolRuntimeException {
		requireSplit();
		return masterJobs;
	}
	
	public Collection<? extends Serializable> getWorkerJobs() throws EolRuntimeException {
		requireSplit();
		return workerJobs;
	}
	
	public Entry<Collection<?>, Collection<? extends Serializable>> split(final List<?> allJobs) throws EolRuntimeException {
		final boolean isBatch = isBatchBased();
		final List<?> actualJobs = isBatch ? getBatchJobs(allJobs) : allJobs;

		if (shuffleJobs) Collections.shuffle(actualJobs);
		
		final int
			numTotalJobs = actualJobs.size(),
			distributedParallelism = context.getDistributedParallelism();		
		
		if (distributedParallelism == 0) {
			masterProportion = 1;
		}
		else if (masterProportion > 1 || masterProportion < 0) {
			masterProportion = distributedParallelism > 0 ? 1/(1.0d + distributedParallelism) : 0;
		}
		
		int numMasterJobs = (int) (masterProportion * numTotalJobs);
		if (numMasterJobs >= numTotalJobs) {
			masterJobs = actualJobs;
			workerJobs = Collections.emptyList();
		}
		else {
			Collection<?> toConvert;
			if (numMasterJobs <= 0) {
				masterJobs = Collections.emptyList();
				toConvert = actualJobs;
			}
			else {
				masterJobs = actualJobs.subList(0, numMasterJobs);
				toConvert = actualJobs.subList(numMasterJobs, numTotalJobs);
			}
			
			@SuppressWarnings("unchecked")
			Collection<? extends Serializable> wj = isBatch ?
				(List<? extends Serializable>) toConvert :
				convertToWorkerJobs(toConvert, context);
			
			workerJobs = wj instanceof Serializable ? wj : new ArrayList<>(wj);
		}
		assert masterJobs.size() + workerJobs.size() == numTotalJobs : "Correct number of jobs";
		return new SimpleImmutableEntry<>(masterJobs, workerJobs);
	}
	
	protected Collection<? extends Serializable> convertToWorkerJobs(Collection<?> jobs, EvlContextDistributedMaster context) throws EolRuntimeException {
		ArrayList<Serializable> wjs = new ArrayList<>(jobs.size());
		
		for (Object job : jobs) {
			if (job instanceof Serializable) {
				wjs.add((Serializable) job);
			}
			else if (job instanceof EvlAtom) {
				wjs.add(SerializableEvlInputAtom.serializeJob((EvlAtom<?>) job, context));
			}
		}
		
		return wjs;
	}
	
	@Override
	public String toString() {
		return "JobSplitter [shuffleJobs=" + shuffleJobs +
			", masterProportion=" + masterProportion +
			", batchSize=" + batchSize + "]";
	}

	public JobSplitter() {
		this(true);
	}
	public JobSplitter(boolean randomJobOrder) {
		this(randomJobOrder, -Double.MAX_VALUE);
	}
	public JobSplitter(boolean randomJobOrder, double masterProportion) {
		this(randomJobOrder, masterProportion, -Double.MAX_VALUE);
	}
}
