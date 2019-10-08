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

public class JobSplitter {

	static final double UNINTIALIZED_VALUE = -Double.MAX_VALUE;
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
		
		if (Math.min(0, batchSize) < 0) {
			batchSize = context.getParallelism();
		}
		else if (batchSize == 0) {
			batchSize = 1;
		}
		
		if (batchSize >= 1) {
			chunks = (int) batchSize;
		}
		else {
			final int adjusted = Math.max(context.getDistributedParallelism(), 1);
			chunks = (int) (numTotalJobs * (batchSize / adjusted));
		}
		return JobBatch.getBatches(numTotalJobs, chunks);
	}

	protected void screenSplit() throws IllegalArgumentException {
		if (masterJobs == null || workerJobs == null) {
			throw new IllegalStateException("Must call split() first!");
		}
	}
	
	public Collection<?> getMasterJobs() throws EolRuntimeException {
		screenSplit();
		return masterJobs;
	}
	
	public Collection<? extends Serializable> getWorkerJobs() throws EolRuntimeException {
		screenSplit();
		return workerJobs;
	}
	
	public Entry<Collection<?>, Collection<? extends Serializable>> split(List<?> allJobs) throws EolRuntimeException {
		if (shuffleJobs) Collections.shuffle(allJobs);
		
		final int
			numTotalJobs = allJobs.size(),
			distributedParallelism = context.getDistributedParallelism();		
		
		if (distributedParallelism == 0) {
			masterProportion = 1;
		}
		else if (masterProportion > 1 || masterProportion < 0) {
			masterProportion = 1/(1.0d + (double) distributedParallelism);
		}
		
		int numMasterJobs = (int) (masterProportion * numTotalJobs);
		if (numMasterJobs >= numTotalJobs) {
			masterJobs = allJobs;
			workerJobs = Collections.emptyList();
		}
		else {
			Collection<?> toConvert;
			if (numMasterJobs <= 0) {
				masterJobs = Collections.emptyList();
				toConvert = allJobs;
			}
			else {
				masterJobs = allJobs.subList(0, numMasterJobs);
				toConvert = allJobs.subList(numMasterJobs, numTotalJobs);
			}
			
			Collection<? extends Serializable> wj = convertToWorkerJobs(toConvert, context);
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
	
	public JobSplitter() {
		this(true);
	}
	public JobSplitter(boolean randomJobOrder) {
		this(randomJobOrder, UNINTIALIZED_VALUE);
	}
	public JobSplitter(boolean randomJobOrder, double masterProportion) {
		this(randomJobOrder, masterProportion, UNINTIALIZED_VALUE);
	}
}
