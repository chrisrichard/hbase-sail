package org.openrdf.sail.hbase.model;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.sail.hbase.data.ValueStoreRevision;
import org.openrdf.sail.hbase.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseBNode implements HBaseResource, BNode {
	private static final long serialVersionUID = 861142250999359435L;
	private static final Logger logger = LoggerFactory.getLogger(HBaseBNode.class);	
	
	private volatile ValueStoreRevision revision;
	private volatile byte[] internalID;
	private volatile BNode internalBNode;

	/*--------------*
	 * Constructors *
	 *--------------*/

	protected HBaseBNode(ValueStoreRevision revision, byte[] internalID) {
		this(revision, internalID, null);
	}

	public HBaseBNode(ValueStoreRevision revision, String nodeID) {
		this(revision, null, new BNodeImpl(nodeID));
	}

	public HBaseBNode(ValueStoreRevision revision, byte[] internalID, BNode bNode) {
		this.revision = revision;
		
		if (internalID != null || bNode != null) {
			this.setInternalID(internalID, revision);
			this.internalBNode = bNode;
		}
		else {
			throw new IllegalStateException("At least one of internalID or bNode must not be null.");
		}
	}
	
	/*---------*
	 * Methods *
	 *---------*/

	@Override
	public String toString()
	{
		if (this.internalBNode != null)
			return this.internalBNode.toString();
		else
			return "URI #" + StringUtils.byteToHexString(this.internalID);
	}

	public void setInternalID(byte[] internalID, ValueStoreRevision revision) {
		this.internalID = internalID;
		this.revision = revision;
	}

	public byte[] getInternalID() {
		if (internalID == null)
		{
			try	{
				this.internalID = this.revision.getValueStore().getID(this.internalBNode);
			}
			catch (IOException ioe)	{
				logger.error("Failed to get ID for BNode: " + this.internalBNode, ioe);
			}
		}
		
		return internalID;
	}
	
	public ValueStoreRevision getValueStoreRevision() {
		return revision;
	}	

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof HBaseBNode) {
			HBaseBNode otherHBaseBNode = (HBaseBNode)o;

			if (otherHBaseBNode.internalID != null
					&& revision.equals(otherHBaseBNode.revision))
			{
				// NativeBNode's from the same revision of the same native store,
				// with both ID's set
				return Bytes.compareTo(internalID, otherHBaseBNode.internalID) == 0;
			}
		}

		return internalBNode.equals(o);
	}
	
	@Override
	public String getID() {
		this.getInternalBNode();
		return this.internalBNode.getID();
	}

	@Override
	public String stringValue() {
		this.getInternalBNode();
		return this.internalBNode.stringValue();
	}
	
	private void getInternalBNode()
	{
		if (this.internalBNode == null)		
			this.internalBNode = (BNode)revision.getValueStore().getInternalValue(this.internalID);
	}
}