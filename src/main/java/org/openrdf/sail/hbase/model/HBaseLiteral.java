package org.openrdf.sail.hbase.model;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.sail.hbase.data.ValueStoreRevision;
import org.openrdf.sail.hbase.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseLiteral implements HBaseValue, Literal {
	private static final long serialVersionUID = -8213249522968522279L;
	private static final Logger logger = LoggerFactory.getLogger(HBaseLiteral.class);
	
	private volatile ValueStoreRevision revision;
	private volatile byte[] internalID;
	private volatile Literal internalLiteral;

	/*--------------*
	 * Constructors *
	 *--------------*/

	protected HBaseLiteral(ValueStoreRevision revision, byte[] internalID) {
		this(revision, internalID, null);
	}

	public HBaseLiteral(ValueStoreRevision revision, String label) {
		this(revision, null, new LiteralImpl(label));
	}

	public HBaseLiteral(ValueStoreRevision revision, String label, String lang) {
		this(revision, null, new LiteralImpl(label, lang));
	}

	public HBaseLiteral(ValueStoreRevision revision, String label, URI datatype) {
		this(revision, null, new LiteralImpl(label, datatype));
	}

	public HBaseLiteral(ValueStoreRevision revision, byte[] internalID, Literal literal)
	{
		this.revision = revision;
		
		if (internalID != null || literal != null) {
			this.setInternalID(internalID, revision);
			this.internalLiteral = literal;
		}
		else {
			throw new IllegalStateException("At least one of internalID or literal must not be null.");
		}
	}

	/*---------*
	 * Methods *
	 *---------*/
	
	@Override
	public String toString()
	{
		if (this.internalLiteral != null)
			return this.internalLiteral.toString();
		else
			return "Literal #" + StringUtils.byteToHexString(this.internalID);
	}

	public void setInternalID(byte[] internalID, ValueStoreRevision revision) {
		this.internalID = internalID;
		this.revision = revision;
	}

	public byte[] getInternalID() {
		if (internalID == null)
		{
			try	{
				this.internalID = this.revision.getValueStore().getID(this.internalLiteral);
			}
			catch (IOException ioe)	{
				logger.error("Failed to get ID for Literal: " + this.internalLiteral, ioe);
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

		if (o instanceof HBaseLiteral) {
			HBaseLiteral otherHBaseLiteral = (HBaseLiteral)o;

			if (otherHBaseLiteral.internalID != null
					&& revision.equals(otherHBaseLiteral.revision))
			{
				// HBaseLiterals from the same revision of the same HBase store,
				// with both IDs set
				return Bytes.compareTo(internalID, otherHBaseLiteral.internalID) == 0;
			}
		}

		return internalLiteral.equals(o);
	}

	@Override
	public String stringValue() {
		getInternalLiteral();
		return this.internalLiteral.stringValue();
	}

	@Override
	public boolean booleanValue() {
		getInternalLiteral();
		return this.internalLiteral.booleanValue();
	}

	@Override
	public byte byteValue() {
		getInternalLiteral();
		return this.internalLiteral.byteValue();
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		getInternalLiteral();
		return this.internalLiteral.calendarValue();
	}

	@Override
	public BigDecimal decimalValue() {
		getInternalLiteral();
		return this.internalLiteral.decimalValue();
	}

	@Override
	public double doubleValue() {
		getInternalLiteral();
		return this.internalLiteral.doubleValue();
	}

	@Override
	public Duration durationValue() {
		getInternalLiteral();
		return this.internalLiteral.durationValue();
	}

	@Override
	public float floatValue() {
		getInternalLiteral();
		return this.internalLiteral.floatValue();
	}

	@Override
	public URI getDatatype() {
		getInternalLiteral();
		return this.internalLiteral.getDatatype();
	}

	@Override
	public String getLabel() {
		getInternalLiteral();
		return this.internalLiteral.getLabel();
	}

	@Override
	public String getLanguage() {
		getInternalLiteral();
		return this.internalLiteral.getLanguage();
	}

	@Override
	public int intValue() {
		getInternalLiteral();
		return this.internalLiteral.intValue();
	}

	@Override
	public BigInteger integerValue() {
		getInternalLiteral();
		return this.internalLiteral.integerValue();
	}

	@Override
	public long longValue() {
		getInternalLiteral();
		return this.internalLiteral.longValue();
	}

	@Override
	public short shortValue() {
		getInternalLiteral();
		return this.internalLiteral.shortValue();
	}
	
	private void getInternalLiteral()
	{
		if (this.internalLiteral == null)		
			this.internalLiteral = (Literal)revision.getValueStore().getInternalValue(this.internalID);
	}	
}