package org.openrdf.sail.hbase.config;

import static org.openrdf.sail.hbase.config.HBaseStoreSchema.CATALOG_NAME;
import static org.openrdf.sail.hbase.config.HBaseStoreSchema.FAMILY_FIELD_BITS;
import static org.openrdf.sail.hbase.config.HBaseStoreSchema.KEY_FIELDS;
import static org.openrdf.sail.hbase.config.HBaseStoreSchema.QUALIFIER_FIELDS;
import static org.openrdf.sail.hbase.config.HBaseStoreSchema.TRIPLE_INDEX;
import static org.openrdf.sail.hbase.config.HBaseStoreSchema.VALUE_FIELDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.openrdf.model.BNode;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.ModelException;
import org.openrdf.sail.config.SailImplConfigBase;
import org.openrdf.store.StoreConfigException;

public class HBaseStoreConfig extends SailImplConfigBase {
	
	public static final ArrayList<IndexSpec> DEFAULT_INDEXES = GetDefaultIndexes();
	private static ArrayList<IndexSpec> GetDefaultIndexes()
	{
		ArrayList<IndexSpec> indexSpecs = new ArrayList<IndexSpec>(2);
		indexSpecs.add(new IndexSpec("csp", "o", null, "0060"));
		indexSpecs.add(new IndexSpec("opc", "s", null, "0600"));
		
		return indexSpecs;
	}
	
	private String catalogName;
	private ArrayList<IndexSpec> tripleIndexes;
	
	/*--------------*
	 * Constructors *
	 *--------------*/

	public HBaseStoreConfig() {
		this(null, DEFAULT_INDEXES);
	}
	
	public HBaseStoreConfig(String catalogName) {
		this(catalogName, DEFAULT_INDEXES);
	}

	public HBaseStoreConfig(String catalogName, ArrayList<IndexSpec> tripleIndexes) {
		super(HBaseStoreFactory.SAIL_TYPE);
	
		this.catalogName = catalogName;
		if (tripleIndexes != null)
			this.tripleIndexes = tripleIndexes;
		else
			this.tripleIndexes = DEFAULT_INDEXES;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public String getCatalogName() {
		return catalogName;
	}

	public List<IndexSpec> getTripleIndexes() {
		return Collections.unmodifiableList(tripleIndexes);
	}

	@Override
	public Resource export(Model model) {
		Resource implNode = super.export(model);

		ValueFactoryImpl vf = ValueFactoryImpl.getInstance();

		model.add(implNode, CATALOG_NAME, vf.createLiteral(catalogName));
		for (IndexSpec indexSpec : tripleIndexes) {
			BNode indexNode = vf.createBNode();
			model.add(indexNode, KEY_FIELDS, vf.createLiteral(new String(indexSpec.getKeyFields())));
			model.add(indexNode, QUALIFIER_FIELDS, vf.createLiteral(new String(indexSpec.getQualifierFields())));
			model.add(indexNode, VALUE_FIELDS, vf.createLiteral(new String(indexSpec.getValueFields())));
			
			model.add(implNode, TRIPLE_INDEX, indexNode);
		}

		return implNode;
	}

	@Override
	public void parse(Model model, Resource implNode)
		throws StoreConfigException
	{
		super.parse(model, implNode);

		try {
			catalogName = model.filter(implNode, CATALOG_NAME, null).objectString();
			Iterator<Statement> indexSpecs = model.filter(implNode, TRIPLE_INDEX, null).iterator();
			if (indexSpecs.hasNext()) {
				tripleIndexes.clear();
			
				while (indexSpecs.hasNext()) {
					Statement indexSpec = indexSpecs.next();
					String keyFields = model.filter((Resource)indexSpec.getObject(), KEY_FIELDS, null).objectLiteral().getLabel();
					String qualifierFields = model.filter((Resource)indexSpec.getObject(), QUALIFIER_FIELDS, null).objectLiteral().getLabel();
					String valueFields = model.filter((Resource)indexSpec.getObject(), VALUE_FIELDS, null).objectLiteral().getLabel();
					String familyFieldBits = model.filter((Resource)indexSpec.getObject(), FAMILY_FIELD_BITS, null).objectLiteral().getLabel();
					
					tripleIndexes.add(new IndexSpec(keyFields, qualifierFields, valueFields, familyFieldBits));
				}
			}
		}
		catch (ModelException e) {
			throw new StoreConfigException(e.getMessage(), e);
		}
	}	
}
