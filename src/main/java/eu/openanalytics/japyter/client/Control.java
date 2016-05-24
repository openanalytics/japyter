/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

/**
 * Control is exactly like Shell except that it goes through a privileged queue.
 */
public class Control extends Shell
{
    public Control(final String address, final Session session)
    {
        super(address, session);
    }
}
