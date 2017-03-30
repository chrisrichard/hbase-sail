package org.openrdf.sail.hbase.config;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

public class HBaseStoreSchema {
	public static final String NAMESPACE = "http://www.openrdf.org/config/sail/hbase#";

	/** <tt>http://www.openrdf.org/config/sail/hbase#catalogName</tt> */
	public final static URI CATALOG_NAME;

	/** <tt>http://www.openrdf.org/config/sail/hbase#tripleIndex</tt> */
	public final static URI TRIPLE_INDEX;

	/** <tt>http://www.openrdf.org/config/sail/hbase#keyFields</tt> */
	public final static URI KEY_FIELDS;
	
	/** <tt>http://www.openrdf.org/config/sail/hbase#columnFields</tt> */
	public final static URI QUALIFIER_FIELDS;
	
	/** <tt>http://www.openrdf.org/config/sail/hbase#valueFields</tt> */
	public final static URI VALUE_FIELDS;

	/** <tt>http://www.openrdf.org/config/sail/hbase#familyFieldBits</tt> */
	public final static URI FAMILY_FIELD_BITS;
	
	static {
		ValueFactory factory = ValueFactoryImpl.getInstance();
		
		CATALOG_NAME = factory.createURI(NAMESPACE, "catalogName");
		
		TRIPLE_INDEX = factory.createURI(NAMESPACE, "tripleIndex");
		
		KEY_FIELDS = factory.createURI(NAMESPACE, "keyFields");
		
		QUALIFIER_FIELDS = factory.createURI(NAMESPACE, "columnFields");
		
		VALUE_FIELDS = factory.createURI(NAMESPACE, "valueFields");
		
		FAMILY_FIELD_BITS = factory.createURI(NAMESPACE, "familyFieldBits");
	}
}
