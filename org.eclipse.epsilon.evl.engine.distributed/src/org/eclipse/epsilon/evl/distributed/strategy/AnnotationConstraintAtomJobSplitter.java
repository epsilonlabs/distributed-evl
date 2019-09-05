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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintAtom;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class AnnotationConstraintAtomJobSplitter extends JobSplitter<ConstraintAtom, SerializableEvlInputAtom> {

	public static final String DISTRIBUTED_ANNOTATION_NAME = "distributed";
	
	public AnnotationConstraintAtomJobSplitter(EvlContextDistributedMaster context, boolean shuffle) {
		super(context, shuffle);
	}
	
	@Override
	protected void split() throws EolRuntimeException {
		List<ConstraintAtom> allJobs = getAllJobs();
		if (shuffle) Collections.shuffle(allJobs);
		int numTotalJobs = allJobs.size();
		
		masterJobs = new ArrayList<>(numTotalJobs/2);
		ArrayList<ConstraintAtom> workersLocal = new ArrayList<>(numTotalJobs/2);
		
		for (ConstraintAtom ca : allJobs) {
			if (ca.rule.getBooleanAnnotationValue(DISTRIBUTED_ANNOTATION_NAME, context)) {
				workersLocal.add(ca);
			}
			else {
				masterJobs.add(ca);
			}
		}
		workerJobs = convertToWorkerJobs(workersLocal);
	}
	
	@Override
	protected List<ConstraintAtom> getAllJobs() throws EolRuntimeException {
		return ConstraintAtom.getConstraintJobs(context.getModule());
	}
	
	@Override
	protected ArrayList<SerializableEvlInputAtom> convertToWorkerJobs(List<ConstraintAtom> masterJobs) throws EolRuntimeException {
		ArrayList<SerializableEvlInputAtom> workerJobs = new ArrayList<>(masterJobs.size());
		for (ConstraintAtom ca : masterJobs) {
			EolModelElementType modelType = ca.rule.getConstraintContext().getType(context);
			SerializableEvlInputAtom seia = new SerializableEvlInputAtom();
			seia.modelElementID = modelType.getModel().getElementId(ca.element);
			seia.contextName = ca.rule.getConstraintContext().getTypeName();
			seia.modelName = modelType.getModelName();
			seia.constraintName = ca.rule.getName();
			workerJobs.add(seia);
		}
		return workerJobs;
	}

}
