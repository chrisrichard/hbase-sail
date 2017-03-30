package org.openrdf.sail.hbase.model;

import java.io.IOException;
import java.util.Arrays;

import org.openrdf.model.Value;
import org.openrdf.sail.hbase.HBaseException;
import org.openrdf.sail.hbase.data.ValueStoreRevision;
import org.openrdf.sail.hbase.data.ValueTable;

public interface HBaseValue extends Value {

	/**
	 * Sets the ID that is used for this value in a specific revision of the
	 * value store.
	 */
	public void setInternalID(byte[] id, ValueStoreRevision revision);

	/**
	 * Gets the ID that is used in the native store for this Value.
	 * 
	 * @return The value's ID, or {@link #UNKNOWN_ID} if not yet set.
	 */
	public byte[] getInternalID();

	/**
	 * Gets the revision of the value store that created this value. The value's
	 * internal ID is only valid when it's value store revision is equal to the
	 * value store's current revision.
	 * 
	 * @return The revision of the value store that created this value at the
	 *         time it last set the value's internal ID.
	 */
	public ValueStoreRevision getValueStoreRevision();
}