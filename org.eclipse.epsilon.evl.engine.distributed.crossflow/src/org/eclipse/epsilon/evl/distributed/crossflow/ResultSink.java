/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class ResultSink extends ResultSinkBase {
	
	@Override
	public void consumeValidationOutput(ValidationResult validationResult) throws Exception {
		workflow.configConfigSource.module.getContext().executeJob(validationResult);
	}

}