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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class AnnotationJobSplitter<T, S extends Serializable> extends JobSplitter<T, S> {

	public static final String DISTRIBUTED_ANNOTATION_NAME = "distributed";
	
	public AnnotationJobSplitter(EvlContextDistributedMaster context) {
		this(context, false);
	}
	
	public AnnotationJobSplitter(EvlContextDistributedMaster context, boolean shuffle) {
		super(context, shuffle);
	}

	@Override
	protected void split() throws EolRuntimeException {
		List<T> allJobs = getAllJobs();
		if (shuffle) Collections.shuffle(allJobs);
		int numTotalJobs = allJobs.size();
		
		masterJobs = new ArrayList<>(numTotalJobs/2);
		ArrayList<T> workersLocal = new ArrayList<>(numTotalJobs/2);
		
		for (T job : allJobs) {
			if (shouldBeDistributed(job)) {
				workersLocal.add(job);
			}
			else {
				masterJobs.add(job);
			}
		}
		workerJobs = convertToWorkerJobs(workersLocal);
	}

	protected abstract boolean shouldBeDistributed(T job) throws EolRuntimeException;
	
}
