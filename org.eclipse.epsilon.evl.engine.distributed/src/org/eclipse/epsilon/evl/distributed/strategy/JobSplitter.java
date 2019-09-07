/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.strategy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

public abstract class JobSplitter<T, S extends Serializable> {
	
	protected final EvlContextDistributedMaster context;
	protected final double masterProportion;
	protected final boolean shuffle;
	protected ArrayList<S> workerJobs;
	protected List<T> masterJobs;
	
	protected JobSplitter() {
		this(null);
	}
	protected JobSplitter(boolean shuffle) {
		this(null, shuffle);
	}
	protected JobSplitter(EvlContextDistributedMaster context) {
		this(context, false);
	}
	protected JobSplitter(EvlContextDistributedMaster context, boolean shuffle) {
		this.shuffle = shuffle;
		this.context = context;
		this.masterProportion = -1;
	}
	
	/**
	 * 
	 * @param masterProportion The percentage of jobs to be performed by the master. Must be between 0 and 1.
	 * @param shuffle Whether to randomise thr order of jobs.
	 * @throws IllegalArgumentException If the percentage is out of bounds.
	 */
	public JobSplitter(EvlContextDistributedMaster context, double masterProportion, boolean shuffle) {
		this.shuffle = shuffle;
		this.context = context;
		this.masterProportion = sanitizeMasterProportion(masterProportion);
	}
	
	/**
	 * Validates the masterProportion parameter, providing a default fallback value if out of range.
	 * 
	 * @param percent01 The supplied masterProportion argument.
	 * @return A value between 0 and 1.
	 */
	protected double sanitizeMasterProportion(double percent01) {
		if (context.getDistributedParallelism() == 0) return 1;
		return Math.max(percent01, 1) > 1 || Math.min(percent01, 0) < 0 ?
			1/(1+context.getDistributedParallelism()) : percent01;
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