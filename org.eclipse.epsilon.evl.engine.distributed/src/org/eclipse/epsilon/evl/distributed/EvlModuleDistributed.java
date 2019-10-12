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

import java.util.Objects;
import org.eclipse.epsilon.evl.concurrent.atomic.EvlModuleParallelContextAtoms;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributed;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributed extends EvlModuleParallelContextAtoms {

	public EvlModuleDistributed(EvlContextDistributed context) {
		super(Objects.requireNonNull(context));
	}
	
	@Override
	public EvlContextDistributed getContext() {
		return (EvlContextDistributed) super.getContext();
	}
}
