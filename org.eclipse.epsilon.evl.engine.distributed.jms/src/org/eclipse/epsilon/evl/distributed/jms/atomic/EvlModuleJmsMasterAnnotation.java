/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.atomic;

import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
import org.eclipse.epsilon.evl.distributed.strategy.AnnotationConstraintAtomJobSplitter;

/**
 * Atom-based approach, sending the Serializable Constraint and model element
 * pairs to workers which are annotated with {@linkplain AnnotationConstraintAtomJobSplitter#DISTRIBUTED_ANNOTATION_NAME}.
 * 
 * @see SerializableEvlInputAtom
 * @see ConstraintAtom
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleJmsMasterAnnotation extends EvlModuleJmsMasterAtomic {
	
	public EvlModuleJmsMasterAnnotation(EvlContextJmsMaster context, AnnotationConstraintAtomJobSplitter  strategy) {
		super(context, strategy);
	}
}
