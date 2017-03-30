package org.openrdf.sail.hbase.model;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.hbase.ValueStore;
import org.openrdf.sail.hbase.data.ValueStoreRevision;
import org.openrdf.sail.hbase.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

 public class HBaseURI implements URI, HBaseResource {
	private static final long serialVersionUID = 3317398596013196032L;
	private static final Logger logger = LoggerFactory.getLogger(HBaseURI.class);	
	
	private volatile ValueStoreRevision revision;

	private volatile byte[] internalID;
	private volatile URI internalURI;

	/*--------------*
	 * Constructors *
	 *--------------*/

	protected HBaseURI(ValueStoreRevision revision, byte[] internalID) {
		this(revision, internalID, null);
	}

	public HBaseURI(ValueStoreRevision revision, String uri) {
		this(revision, (byte[])null, new URIImpl(uri));
	}
	
	public HBaseURI(ValueStoreRevision revision, byte[] internalID, URI uri)
	{
		this.revision = revision;
		
		if (internalID != null || uri != null) {
			this.setInternalID(internalID, revision);
			this.internalURI = uri;
		}
		else {
			throw new IllegalStateException("At least one of internalID or uri must not be null.");
		}
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	public String toString()
	{
		if (this.internalURI != null)
			return this.internalURI.toString();
		else
			return "URI #" + StringUtils.byteToHexString(this.internalID);
	}

	public ValueStoreRevision getValueStoreRevision() {
		return revision;
	}
	
	public void setInternalID(byte[] internalID, ValueStoreRevision revision) {
		this.internalID = internalID;
		this.revision = revision;
	}

	public byte[] getInternalID() {
		if (internalID == null)
		{
			try	{
				this.internalID = this.revision.getValueStore().getID(this.internalURI);
			}
			catch (IOException ioe)	{
				logger.error("Failed to get internal ID for URI: " + this.internalURI, ioe);
			}
		}
		
		return internalID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof HBaseURI) {
			HBaseURI otherHBaseURI = (HBaseURI)o;

			if (otherHBaseURI.internalID != null && revision.equals(otherHBaseURI.revision))
			{
				// HBaseURI's from the same revision of the same HBase store, with
				// both ID's set
				return Bytes.compareTo(internalID, otherHBaseURI.internalID) == 0;
			}
		}

		return internalURI.equals(o);
	}

	@Override
	public String getLocalName() {
		this.getInternalURI();
		return this.internalURI.getLocalName();
	}

	@Override
	public String getNamespace() {
		this.getInternalURI();
		return this.internalURI.getNamespace();
	}

	@Override
	public String stringValue() {
		this.getInternalURI();
		return this.internalURI.stringValue();
	}
	
	private void getInternalURI()
	{
		if (this.internalURI == null) 	
			this.internalURI = (URI)revision.getValueStore().getInternalValue(this.internalID);
	}
}
