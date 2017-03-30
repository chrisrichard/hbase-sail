package org.openrdf.sail.hbase;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.store.StoreException;

public class HBaseException extends StoreException {

	private static Logger logger = LoggerFactory.getLogger(HBaseException.class);

	private static final long serialVersionUID = -4004800841908629773L;

	private static SQLException findInterestingCause(SQLException e) {
		if (e instanceof BatchUpdateException) {
			BatchUpdateException b = (BatchUpdateException)e;
			logger.error(b.toString(), b);
			return b.getNextException();
		}
		return e;
	}

	public HBaseException(SQLException e) {
		super(findInterestingCause(e));
	}

	public HBaseException(String msg) {
		super(msg);
	}

	public HBaseException(String msg, Exception e) {
		super(msg, e);
	}

	public HBaseException(Exception e) {
		super(e);
	}

}
