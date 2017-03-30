package org.openrdf.sail.hbase.config;

public class IndexSpec {

	private char[] fieldSeq;

	public char[] getFieldSeq() {
		return fieldSeq;
	}

	private char[] keyFields;

	public char[] getKeyFields() {
		return keyFields;
	}

	private char[] qualifierFields;

	public char[] getQualifierFields() {
		return qualifierFields;
	}

	private char[] valueFields;

	public char[] getValueFields() {
		return valueFields;
	}

	private byte[] familyFieldBits;

	public byte[] getFamilyFieldBits() {
		return familyFieldBits;
	}

	public IndexSpec(String keyFields, String qualifierFields,
			String valueFields, String familyFieldBits) {
		
		this.fieldSeq = 
			(keyFields
				+ (qualifierFields != null ? qualifierFields : "") 
				+ (valueFields != null ? valueFields : ""))
					.toCharArray();

		this.keyFields = keyFields.toCharArray();
		this.qualifierFields = qualifierFields != null ? qualifierFields.toCharArray() : null;
		this.valueFields = valueFields != null ? valueFields.toCharArray() : null;

		this.familyFieldBits = new byte[familyFieldBits.length()];
		for (int i = 0; i < familyFieldBits.length(); ++i) {
			this.familyFieldBits[i] = Byte.parseByte(new String(familyFieldBits.toCharArray(), i, 1));
		}
	}
}
