/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.hbase.data;

import java.io.Serializable;

import org.openrdf.sail.hbase.ValueStore;

/**
 * A {@link ValueStore ValueStore} revision for {@link HBaseValue HBaseValue}
 * objects. For a cached value ID of a HBaseValue to be valid, the revision
 * object needs to be equal to the concerning ValueStore's revision object. The
 * ValueStore's revision object is changed whenever values are removed from it
 * or IDs are changed.
 * 
 * @author Arjohn Kampman
 */
public class ValueStoreRevision implements Serializable {

	/*-----------*
	 * Constants *
	 *-----------*/

	private static final long serialVersionUID = -2434063125560285009L;

	/*-----------*
	 * Variables *
	 *-----------*/

	transient private final ValueStore valueStore;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public ValueStoreRevision(ValueStore valueStore) {
		this.valueStore = valueStore;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public ValueStore getValueStore() {
		return valueStore;
	}
}