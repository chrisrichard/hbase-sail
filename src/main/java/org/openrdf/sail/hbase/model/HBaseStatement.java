package org.openrdf.sail.hbase.model;

import org.apache.hadoop.hbase.HConstants;
import org.openrdf.model.impl.StatementImpl;

public class HBaseStatement extends StatementImpl {
	
	private long timestamp;

	public HBaseStatement(HBaseResource subject, HBaseURI predicate, HBaseValue object) {
		this(subject, predicate, object, null);
	}

	public HBaseStatement(HBaseResource subject, HBaseURI predicate, HBaseValue object,
			HBaseResource context) {
		this(subject, predicate, object, context, HConstants.LATEST_TIMESTAMP);
	}
	
	public HBaseStatement(HBaseResource subject, HBaseURI predicate, HBaseValue object,
			HBaseResource context, long timestamp) {
		super(subject, predicate, object, context);
		this.timestamp = timestamp;
	}

	@Override
	public HBaseResource getSubject() {
		return (HBaseResource) super.getSubject();
	}

	@Override
	public HBaseURI getPredicate() {
		return (HBaseURI) super.getPredicate();
	}

	@Override
	public HBaseValue getObject() {
		return (HBaseValue) super.getObject();
	}

	@Override
	public HBaseResource getContext() {
		return (HBaseResource) super.getContext();
	}
}
