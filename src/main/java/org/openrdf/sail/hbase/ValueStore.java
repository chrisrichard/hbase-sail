package org.openrdf.sail.hbase;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.BNodeFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeFactoryImpl;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralFactoryImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.hbase.data.HBaseTableFactory;
import org.openrdf.sail.hbase.data.ValueStoreRevision;
import org.openrdf.sail.hbase.data.ValueTable;
import org.openrdf.sail.hbase.model.HBaseBNode;
import org.openrdf.sail.hbase.model.HBaseLiteral;
import org.openrdf.sail.hbase.model.HBaseResource;
import org.openrdf.sail.hbase.model.HBaseURI;
import org.openrdf.sail.hbase.model.HBaseValue;
import org.openrdf.sail.hbase.util.ByteArray;
import org.openrdf.sail.hbase.util.LRUCache;
import org.openrdf.sail.hbase.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueStore extends LiteralFactoryImpl implements ValueFactory {
	private static final Logger logger = LoggerFactory.getLogger(ValueStore.class);

	private static final int VALUE_CACHE_SIZE = 512;
	private static final int VALUE_ID_CACHE_SIZE = 128;

	private static final int NAMESPACE_CACHE_SIZE = 64;
	private static final int NAMESPACE_ID_CACHE_SIZE = 32;	
	
	private HBaseStore store;
	private ValueTable values;
	
	private BNodeFactory bnodes = new BNodeFactoryImpl();

	/**
	 * An object that indicates the revision of the value store, which is used to
	 * check if cached value IDs are still valid. In order to be valid, the
	 * ValueStoreRevision object of a HBaseValue needs to be equal to this
	 * object.
	 */
	private volatile ValueStoreRevision revision;

	/**
	 * A simple cache containing the [VALUE_CACHE_SIZE] most-recently used values
	 * stored by their ID.
	 */
	private final LRUCache<ByteArray, HBaseValue> valueCache;

	/**
	 * A simple cache containing the [ID_CACHE_SIZE] most-recently used value-IDs
	 * stored by their value.
	 */
	private final LRUCache<Value, byte[]> valueIDCache;

	/**
	 * A simple cache containing the [NAMESPACE_CACHE_SIZE] most-recently used
	 * namespaces stored by their ID.
	 */
	private final LRUCache<ByteArray, String> namespaceCache;

	/**
	 * A simple cache containing the [NAMESPACE_ID_CACHE_SIZE] most-recently used
	 * namespace-IDs stored by their namespace.
	 */
	private final LRUCache<String, byte[]> namespaceIDCache;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public ValueStore(HBaseStore store, ValueTable values)
		throws IOException
	{
		this(store, values, false);
	}

	public ValueStore(HBaseStore store, ValueTable values, boolean forceSync)
		throws IOException
	{
		this.store = store;
		this.values = values;

		valueCache = new LRUCache<ByteArray, HBaseValue>(VALUE_CACHE_SIZE);
		valueIDCache = new LRUCache<Value, byte[]>(VALUE_ID_CACHE_SIZE);
		namespaceCache = new LRUCache<ByteArray, String>(NAMESPACE_CACHE_SIZE);
		namespaceIDCache = new LRUCache<String, byte[]>(NAMESPACE_ID_CACHE_SIZE);

		setNewRevision();
	}

	/*---------*
	 * Methods *
	 *---------*/

	/**
	 * Creates a new revision object for this value store, invalidating any IDs
	 * cached in HBaseValue objects that were created by this value store.
	 */
	private void setNewRevision() {
		revision = new ValueStoreRevision(this);
	}

	public ValueStoreRevision getRevision() {
		return revision;
	}

	/**
	 * Gets the value for the specified ID.
	 * 
	 * @param id
	 *        A value ID.
	 * @return The value for the ID, or <tt>null</tt> no such value could be
	 *         found.
	 * @exception IOException
	 *            If an I/O error occurred.
	 */
	public HBaseValue getValue(byte[] id) throws IOException
	{
		 return getValue(id, (byte)-1);
	}
	
	public HBaseValue getValue(byte[] id, byte type)
		throws IOException
	{
		ByteArray idObject = new ByteArray(id);
		HBaseValue resultValue = null;
				
		// Check value cache
		synchronized (valueCache) {
			resultValue = valueCache.get(idObject);
		}

		if (resultValue == null) {

			// Value not in cache, fetch it from file
			Value value = this.getInternalValue(id, type);
			if (value != null) {
				if (value instanceof URI)
					resultValue = new HBaseURI(revision, id, (URI)value);
				else if (value instanceof BNode)
					resultValue = new HBaseBNode(revision, id, (BNode)value);
				else
					resultValue = new HBaseLiteral(revision, id, (Literal)value);

				// Store value in cache
				synchronized (valueCache) {
					valueCache.put(idObject, resultValue);
				}
			}
		}

		return resultValue;
	}
	
	public Value getInternalValue(byte[] id)
	{
		return this.getInternalValue(id, (byte)-1);
	}
	
	public Value getInternalValue(byte[] id, byte type)
	{
		try {
			KeyValue data = values.getData(id, type);
			if (data != null) {
				Value value = data2value(data);
				return value;
			}
		}
		catch (IOException ioe)	{
			logger.error("Failed to get Value for ID: " + StringUtils.byteToHexString(id), ioe);
		}
				
		return null;
	}

	/**
	 * Gets the ID for the specified value.
	 * 
	 * @param value
	 *        A value.
	 * @return The ID for the specified value, or {@link HBaseValue#UNKNOWN_ID}
	 *         if no such ID could be found.
	 * @exception IOException
	 *            If an I/O error occurred.
	 */
	public byte[] getID(Value value)
		throws IOException
	{
		/*
		// Try to get the internal ID from the value itself
		boolean isOwnValue = isOwnValue(value);

		if (isOwnValue) {
			HBaseValue nativeValue = (HBaseValue)value;

			if (revisionIsCurrent(nativeValue)) {
				byte[] id = nativeValue.getInternalID();

				if (id != null) {
					return id;
				}
			}
		}
		 */
		
		// Check cache
		byte[] cachedID = null;
		synchronized (valueIDCache) {
			cachedID = valueIDCache.get(value);
		}

		if (cachedID != null) {

			/*
			if (isOwnValue) {
				// Store id in value for fast access in any consecutive calls
				((HBaseValue)value).setInternalID(cachedID, revision);
			}
			 */
			return cachedID;
		}

		// ID not cached, search in file
		byte[] data = value2data(value, false);

		if (data != null) {
			byte[] id = values.getID(data);

			if (id != null) {
				//if (isOwnValue) {
				//	// Store id in value for fast access in any consecutive calls
				//	((HBaseValue)value).setInternalID(id, revision);
				//}
				//else {
					// Store id in cache
					synchronized (valueIDCache) {
						valueIDCache.put(value, id);
					}
				//}
			}

			return id;
		}

		return null;
	}
	
	public static byte getValueType(Value value)
	{
		if (value instanceof URI)
			return ValueTable.URI_VALUE;
		else if (value instanceof BNode)
			return ValueTable.BNODE_VALUE;
		else
		{
			Literal lit = (Literal)value;
			if (lit.getLabel().length() < ValueTable.LONG_VALUE_LENGTH)
				return ValueTable.LITERAL_VALUE;
			else
				return ValueTable.LONG_LITERAL_VALUE;
		}
	}

	/**
	 * Stores the supplied value and returns the ID that has been assigned to it.
	 * In case the value was already present, the value will not be stored again
	 * and the ID of the existing value is returned.
	 * 
	 * @param value
	 *        The Value to store.
	 * @return The ID that has been assigned to the value.
	 * @exception IOException
	 *            If an I/O error occurred.
	 */
	public byte[] storeValue(Value value)
		throws IOException
	{
		// Try to get the internal ID from the value itself
		boolean isOwnValue = isOwnValue(value);

		if (isOwnValue) {
			HBaseValue nativeValue = (HBaseValue)value;

			if (revisionIsCurrent(nativeValue)) {
				// Value's ID is still current
				byte[] id = nativeValue.getInternalID();

				if (id != null) {
					return id;
				}
			}
		}

		// ID not stored in value itself, try the ID cache
		byte[] cachedID = null;
		synchronized (valueIDCache) {
			cachedID = valueIDCache.get(value);
		}

		if (cachedID != null) {

			if (isOwnValue) {
				// Store id in value for fast access in any consecutive calls
				((HBaseValue)value).setInternalID(cachedID, revision);
			}

			return cachedID;
		}

		// Unable to get internal ID in a cheap way, just store it in the data
		// store which will handle duplicates
		byte[] valueData = value2data(value, true);
		byte valueType = ValueStore.getValueType(value);

		byte[] id = values.putData(valueData, valueType);

		if (isOwnValue) {
			// Store id in value for fast access in any consecutive calls
			((HBaseValue)value).setInternalID(id, revision);
		}
		else {
			// Update cache
			synchronized (valueIDCache) {
				valueIDCache.put(value, id);
			}
		}

		return id;
	}

	/**
	 * Removes all values from the ValueStore.
	 * 
	 * @exception IOException
	 *            If an I/O error occurred.
	 */
	public void clear()
		throws IOException
	{
		values.clear();
	}

	/**
	 * Checks if the supplied Value object is a HBaseValue object that has been
	 * created by this ValueStore.
	 */
	private boolean isOwnValue(Value value) {
		return value instanceof HBaseValue
				&& ((HBaseValue)value).getValueStoreRevision().getValueStore() == this;
	}

	/**
	 * Checks if the revision of the supplied value object is still current.
	 */
	private boolean revisionIsCurrent(HBaseValue value) {
		return revision.equals(value.getValueStoreRevision());
	}

	private byte[] value2data(Value value, boolean create)
		throws IOException
	{
		if (value instanceof URI) {
			return uri2data((URI)value, create);
		}
		else if (value instanceof BNode) {
			return bnode2data((BNode)value, create);
		}
		else if (value instanceof Literal) {
			return literal2data((Literal)value, create);
		}
		else {
			throw new IllegalArgumentException("value parameter should be a URI, BNode or Literal");
		}
	}

	private byte[] uri2data(URI uri, boolean create)
		throws IOException
	{
		byte[] nsID = getNamespaceID(uri.getNamespace(), create);

		if (nsID == null) {
			// Unknown namespace means unknown URI
			return null;
		}

		// Get local name in UTF-8
		byte[] localNameData = uri.getLocalName().getBytes("UTF-8");

		// Combine parts in a single byte array
		byte[] uriData = new byte[nsID.length + localNameData.length];

		Bytes.putBytes(uriData, 0, nsID, 0, nsID.length);
		Bytes.putBytes(uriData, nsID.length, localNameData, 0, localNameData.length);

		return uriData;
	}

	private byte[] bnode2data(BNode bNode, boolean create)
		throws IOException
	{
		byte[] bNodeData = Bytes.toBytes(bNode.getID());
		return bNodeData;
	}

	private byte[] literal2data(Literal literal, boolean create)
		throws IOException
	{
		// get datatype ID
		byte[] datatypeID = null;
		if (literal.getDatatype() != null) {
			if (create) {
				datatypeID = storeValue(literal.getDatatype());
			}
			else {
				datatypeID = getID(literal.getDatatype());

				if (datatypeID == null) {
					// Unknown datatype means unknown literal
					return null;
				}
			}
		}

		// get language bytes
		byte[] langData = null;
		int langDataLength = 0;
		if (literal.getLanguage() != null) {
			langData = Bytes.toBytes(literal.getLanguage());
			langDataLength = langData.length;
		}

		// get label bytes
		byte[] labelData = Bytes.toBytes(literal.getLabel());

		// Combine parts in a single byte array
		byte[] literalData = new byte[ValueTable.NUM_VALUE_BYTES + 1 + langDataLength + labelData.length];
		
		if (datatypeID != null)
		{
			assert (datatypeID.length == ValueTable.NUM_VALUE_BYTES);
			Bytes.putBytes(literalData, 0, datatypeID, 0, datatypeID.length);
		}
		
		literalData[ValueTable.NUM_VALUE_BYTES] = (byte)langDataLength;
		
		if (langData != null) {
			Bytes.putBytes(literalData, ValueTable.NUM_VALUE_BYTES + 1, langData, 0, langDataLength);
		}
		
		Bytes.putBytes(literalData, ValueTable.NUM_VALUE_BYTES + 1 + langDataLength, labelData, 0, labelData.length);

		return literalData;
	}

	protected Value data2value(KeyValue kv)
		throws IOException
	{
		byte type = ValueTable.getType(kv);
		if (type == ValueTable.URI_VALUE) {
			return data2uri(kv);
		}
		else if (type == ValueTable.BNODE_VALUE) {
			return data2bnode(kv);
		}
		else {
			return data2literal(kv);
		}
		
		//throw new IllegalArgumentException("Data does not specify a known value type.");		
	}

	protected URI data2uri(KeyValue kv)
		throws IOException
	{
		byte[] nsID = ValueStore.getByteArraySlice(kv.getBuffer(), kv.getValueOffset(), ValueTable.NUM_VALUE_BYTES);
		String namespace = getNamespace(nsID);

		String localName = Bytes.toString(kv.getBuffer(), kv.getValueOffset() + ValueTable.NUM_VALUE_BYTES, kv.getValueLength() - ValueTable.NUM_VALUE_BYTES);

		return new URIImpl(namespace + localName);
	}

	protected BNode data2bnode(KeyValue kv)
		throws IOException
	{
		String nodeID = Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
		return new BNodeImpl(nodeID);
	}
	
	private static byte[] getByteArraySlice(byte[] source, int offset, int length)
	{
		byte [] result = new byte[length];
		System.arraycopy(source, offset, result, 0, length);
		return result;
	}
	
	private static byte[] getInternalID(KeyValue kv)
	{
		byte[] internalID = kv.getRow();
		internalID[internalID.length - 1] |= kv.getBuffer()[kv.getQualifierOffset()];
		
		return internalID;
	}
	
	protected Literal data2literal(KeyValue kv)
		throws IOException
	{
		// Get datatype
		byte[] datatypeID = ValueStore.getByteArraySlice(kv.getBuffer(), kv.getValueOffset(), ValueTable.NUM_VALUE_BYTES);
		URI datatype = null;
		if (datatypeID != null) {
			datatype = (URI)getValue(datatypeID, ValueTable.URI_VALUE);
		}

		// Get language tag
		String lang = null;
		int langLength = kv.getBuffer()[kv.getValueOffset() + datatypeID.length];
		if (langLength > 0) {
			lang = Bytes.toString(kv.getBuffer(), kv.getValueOffset() + datatypeID.length + 1, langLength);
		}
		
		// Get label
		String label = Bytes.toString(kv.getBuffer(), 
				kv.getValueOffset() + datatypeID.length + 1 + langLength,
				kv.getValueLength() - (datatypeID.length + 1 + langLength));

		if (datatype != null) {
			return new LiteralImpl(label, datatype);
		}
		else if (lang != null) {
			return new LiteralImpl(label, lang);
		}
		else {
			return new LiteralImpl(label);
		}
	}

	private byte[] getNamespaceID(String namespace, boolean create)
		throws IOException
	{
		byte[] id;

		synchronized (namespaceIDCache) {
			id = namespaceIDCache.get(namespace);
		}

		if (id == null) {
			byte[] namespaceData = Bytes.toBytes(namespace);

			if (create) {
				id = values.putData(namespaceData, ValueTable.URI_VALUE);
			}
			else {
				id = values.getID(namespaceData);
			}

			if (id != null) {
				namespaceIDCache.put(namespace, id);
			}
		}

		return id;
	}

	private String getNamespace(byte[] id)
		throws IOException
	{
		ByteArray idObject = new ByteArray(id);
		String namespace = null;

		synchronized (namespaceCache) {
			namespace = namespaceCache.get(idObject);
		}

		if (namespace == null) {
			byte[] namespaceData = values.getData(id, ValueTable.URI_VALUE).getValue();
			namespace = Bytes.toString(namespaceData);

			synchronized (namespaceCache) {
				namespaceCache.put(idObject, namespace);
			}
		}

		return namespace;
	}

	/*-------------------------------------*
	 * Methods from interface ValueFactory *
	 *-------------------------------------*/

	public HBaseURI createURI(String uri) {
		return new HBaseURI(revision, uri);
	}

	public HBaseURI createURI(String namespace, String localName) {
		return new HBaseURI(revision, namespace + localName);
	}

	public HBaseBNode createBNode() {
		return createBNode(bnodes.createBNode().getID());
	}

	public HBaseBNode createBNode(String nodeID) {
		return new HBaseBNode(revision, nodeID);
	}

	@Override
	public HBaseLiteral createLiteral(String value) {
		return new HBaseLiteral(revision, value);
	}

	@Override
	public HBaseLiteral createLiteral(String value, String language) {
		return new HBaseLiteral(revision, value, language);
	}

	@Override
	public HBaseLiteral createLiteral(String value, URI datatype) {
		return new HBaseLiteral(revision, value, datatype);
	}

	public Statement createStatement(Resource subject, URI predicate, Value object) {
		return new StatementImpl(subject, predicate, object);
	}

	public Statement createStatement(Resource subject, URI predicate, Value object, Resource context) {
		return new StatementImpl(subject, predicate, object, context);
	}

	/*----------------------------------------------------------------------*
	 * Methods for converting model objects to HBaseStore-specific objects * 
	 *----------------------------------------------------------------------*/

	public HBaseValue getHBaseValue(Value value) {
		if (value instanceof Resource) {
			return getHBaseResource((Resource)value);
		}
		else if (value instanceof Literal) {
			return getHBaseLiteral((Literal)value);
		}
		else {
			throw new IllegalArgumentException("Unknown value type: " + value.getClass());
		}
	}

	public HBaseResource getHBaseResource(Resource resource) {
		if (resource instanceof URI) {
			return getHBaseURI((URI)resource);
		}
		else if (resource instanceof BNode) {
			return getHBaseBNode((BNode)resource);
		}
		else {
			throw new IllegalArgumentException("Unknown resource type: " + resource.getClass());
		}
	}

	/**
	 * Creates a HBaseURI that is equal to the supplied URI. This method returns
	 * the supplied URI itself if it is already a HBaseURI that has been created
	 * by this ValueStore, which prevents unnecessary object creations.
	 * 
	 * @return A HBaseURI for the specified URI.
	 */
	public HBaseURI getHBaseURI(URI uri) {
		if (isOwnValue(uri)) {
			return (HBaseURI)uri;
		}

		return new HBaseURI(revision, uri.toString());
	}

	/**
	 * Creates a HBaseBNode that is equal to the supplied bnode. This method
	 * returns the supplied bnode itself if it is already a HBaseBNode that has
	 * been created by this ValueStore, which prevents unnecessary object
	 * creations.
	 * 
	 * @return A HBaseBNode for the specified bnode.
	 */
	public HBaseBNode getHBaseBNode(BNode bnode) {
		if (isOwnValue(bnode)) {
			return (HBaseBNode)bnode;
		}

		return new HBaseBNode(revision, bnode.getID());
	}

	/**
	 * Creates an HBaseLiteral that is equal to the supplied literal. This
	 * method returns the supplied literal itself if it is already a
	 * HBaseLiteral that has been created by this ValueStore, which prevents
	 * unnecessary object creations.
	 * 
	 * @return A HBaseLiteral for the specified literal.
	 */
	public HBaseLiteral getHBaseLiteral(Literal l) {
		if (isOwnValue(l)) {
			return (HBaseLiteral)l;
		}

		if (l.getLanguage() != null) {
			return new HBaseLiteral(revision, l.getLabel(), l.getLanguage());
		}
		else if (l.getDatatype() != null) {
			HBaseURI datatype = getHBaseURI(l.getDatatype());
			return new HBaseLiteral(revision, l.getLabel(), datatype);
		}
		else {
			return new HBaseLiteral(revision, l.getLabel());
		}
	}
	
}
