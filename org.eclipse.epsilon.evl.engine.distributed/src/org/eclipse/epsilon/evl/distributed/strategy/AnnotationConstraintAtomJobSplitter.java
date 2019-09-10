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
import java.util.Collection;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintAtom;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class AnnotationConstraintAtomJobSplitter extends AnnotationJobSplitter<ConstraintAtom, SerializableEvlInputAtom> {

	public AnnotationConstraintAtomJobSplitter(EvlContextDistributedMaster context, boolean shuffle) {
		super(context, shuffle);
	}
	
	@Override
	protected boolean shouldBeDistributed(ConstraintAtom job) throws EolRuntimeException {
		Variable self = Variable.createReadOnlyVariable("self", job.element);
		return job.rule.getBooleanAnnotationValue(DISTRIBUTED_ANNOTATION_NAME, context, self);
	}
	
	@Override
	protected List<ConstraintAtom> getAllJobs() throws EolRuntimeException {
		return ConstraintAtom.getConstraintJobs(context.getModule());
	}
	
	@Override
	protected Collection<SerializableEvlInputAtom> convertToWorkerJobs(Collection<ConstraintAtom> jobs) throws EolRuntimeException {
		ArrayList<SerializableEvlInputAtom> workerJobs = new ArrayList<>(jobs.size());
		for (ConstraintAtom ca : jobs) {
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
