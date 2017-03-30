package org.openrdf.sail.hbase.data;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.sail.hbase.HBaseStore;
import org.openrdf.sail.hbase.config.IndexSpec;
import org.openrdf.sail.hbase.data.filter.StatementFilter;

public class TripleIndex {

		private String tableName;
		private IndexSpec indexSpec;

		public TripleIndex(HBaseStore store, IndexSpec indexSpec) {
			this.tableName = HBaseTableFactory.getTripleTableName(store.getHBaseStoreConfig().getCatalogName(), new String(indexSpec.getFieldSeq()));
			this.indexSpec = indexSpec;
		}

		public String getTableName() {
			return tableName;
		}
		
		public char[] getFieldSeq() {
			return this.indexSpec.getFieldSeq();
		}

		/**
		 * Determines the 'score' of this index on the supplied pattern of
		 * subject, predicate, object and context IDs. The higher the score, the
		 * better the index is suited for matching the pattern. Lowest score is
		 * 0, which means that the index will perform a sequential scan.
		 */
		public int getPatternScore(byte[] subj, byte[] pred, byte[] obj,
				byte[] context) {
			int score = 0;

			for (char field : this.indexSpec.getFieldSeq()) {
				switch (field) {
				case 's':
					if (subj != null) {
						score++;
					} else {
						return score;
					}
					break;
				case 'p':
					if (pred != null) {
						score++;
					} else {
						return score;
					}
					break;
				case 'o':
					if (obj != null) {
						score++;
					} else {
						return score;
					}
					break;
				case 'c':
					if (context != null) {
						score++;
					} else {
						return score;
					}
					break;
				default:
					throw new RuntimeException("invalid character '" + field
							+ "' in field sequence: " + new String(this.indexSpec.getFieldSeq()));
				}
			}

			return score;
		}
				
		private byte[] getComposite(char[] fields, boolean start, boolean nullable,
				byte[] subj, byte[] pred, byte[] obj, byte[] ctx) {
			
			if (fields == null)
				return null;
			
			byte[][] componentBytes = new byte[fields.length][];
			
			int size = 0;
			boolean foundNull = false;			
			for (int fieldNum = 0; fieldNum < fields.length; ++fieldNum) {
				char field = fields[fieldNum];
				switch (field) {
					case 's':
						componentBytes[fieldNum] = subj;
						break;
					case 'p':
						componentBytes[fieldNum] = pred;
						break;
					case 'o':
						componentBytes[fieldNum] = obj;
						break;
					case 'c':
						componentBytes[fieldNum] = ctx;
						break;
					default:
						throw new RuntimeException("Invalid character '" + field
								+ "' in field sequence: " + new String(this.indexSpec.getFieldSeq()));
				}

				if (componentBytes[fieldNum] == null || foundNull) {
					if (nullable && fieldNum == 0)
						return null;
					
					if (start)
						componentBytes[fieldNum] = ValueTable.NULL_VALUE;
					else
						componentBytes[fieldNum] = ValueTable.MAX_VALUE;
					
					if (!foundNull)
						foundNull = true;
				}
				
				size += componentBytes[fieldNum].length;
			}
			
			byte[] compositeBytes = new byte[size];
			int offset = 0;
			for (int fieldNum = 0; fieldNum < fields.length; ++fieldNum) {
				
				Bytes.putBytes(compositeBytes, offset, componentBytes[fieldNum], 0,
						componentBytes[fieldNum].length);
				offset += componentBytes[fieldNum].length;				
			}

			return compositeBytes;
		}

		public byte[] getKey(byte[] subj, byte[] pred, byte[] obj, byte[] ctx,
				boolean start) {

			return getComposite(this.indexSpec.getKeyFields(), start, false, 
					subj, pred, obj, ctx);
			
			/*
			char[] keyFields = this.indexSpec.getKeyFields();						
			byte[] keyBytes = new byte[ValueTable.NUM_VALUE_BYTES * keyFields.length];
			
			int offset = 0;
			boolean foundNull = false;
			byte[] keyComponent = null;
			for (int fieldNum = 0; fieldNum < keyFields.length; ++fieldNum) {
				char field = keyFields[fieldNum];
				switch (field) {
				case 's':
					keyComponent = subj;
					break;
				case 'p':
					keyComponent = pred;
					break;
				case 'o':
					keyComponent = obj;
					break;
				case 'c':
					keyComponent = ctx;
					break;
				default:
					throw new RuntimeException("invalid character '" + field
							+ "' in field sequence: " + new String(this.indexSpec.getFieldSeq()));
				}

				if (keyComponent == null || foundNull) {
					if (start)
						keyComponent = ValueTable.NULL_VALUE;
					else
						keyComponent = ValueTable.MAX_VALUE;
					if (!foundNull)
						foundNull = true;
				}
				
				Bytes.putBytes(keyBytes, offset, keyComponent, 0,
						keyComponent.length);
				offset += keyComponent.length;
			}

			return keyBytes;
			*/
		}

		public byte[] getStartKey(byte[] subj, byte[] pred, byte[] obj,
				byte[] ctx) {
			return getKey(subj, pred, obj, ctx, true);
		}

		public byte[] getEndKey(byte[] subj, byte[] pred, byte[] obj, byte[] ctx) {
			return getKey(subj, pred, obj, ctx, false);
		}

		public byte[] getKey(byte[] subj, byte[] pred, byte[] obj, byte[] ctx) {
			
			if (subj == null || pred == null || obj == null)
				throw new IllegalArgumentException("None of subj, pred, or obj may be null");

			return getKey(subj, pred, obj, ctx, true);
		}

		public byte[][] getFamilies(byte[] subj, byte[] pred, byte[] obj,
				byte[] ctx) {

			byte familyIndex = 0;
			byte familyMask = 0;
			byte position = 0;

			byte[] keyComponent = null;
			byte[] familyFieldBits = this.indexSpec.getFamilyFieldBits();
			byte totalFamilyBits = 0;
			char[] fieldSeq = this.indexSpec.getFieldSeq();
			for (int fieldNum = 0; fieldNum < 4; ++fieldNum) {
				
				byte fieldBits = familyFieldBits[fieldNum];
				if (fieldBits == 0)
					continue;

				totalFamilyBits += fieldBits;
				
				char field = fieldSeq[fieldNum];
				switch (field) {
				case 's':
					keyComponent = subj;
					break;
				case 'p':
					keyComponent = pred;
					break;
				case 'o':
					keyComponent = obj;
					break;
				case 'c':
					keyComponent = ctx; 
					break;
				default:
					throw new RuntimeException("invalid character '" + field
							+ "' in field sequence: " + new String(fieldSeq));
				}

				if (keyComponent != null) {
					// take the specified number of bits from the LSB
					byte keyComponentByte = keyComponent[keyComponent.length - 1];
					familyIndex |= (keyComponentByte & ((1 << fieldBits) - 1)) << position;
					familyMask |= ((1 << fieldBits) - 1) << position;
				}
				
				position += fieldBits;
			}

			familyMask |= (HBaseTableFactory.MAX_FAMILIES - (1 << totalFamilyBits));
			
			byte[][] families = HBaseTableFactory.FAMILIES[familyMask][familyIndex];
			
			/*
			byte[][] families = new byte[1 << Integer.bitCount(missingFamilyIndex)][];
			getFamilies(families, new byte[] { 0 }, familyIndex,
					missingFamilyIndex, (byte)0, familyFieldBits, (byte)0);
			*/
			
			return families;

		}
/*
		private void getFamilies(byte[][] families, byte[] familyCount,
				byte familyIndex, byte missingFamilyIndex, byte indexOffset,
				byte[] familyFieldBits, byte bitsOffset) {

			if (bitsOffset >= familyFieldBits.length) {

				families[familyCount[0]] = HBaseTableFactory.FAMILY_NAMES[familyIndex];
				++familyCount[0];

			} else {

				byte bits = familyFieldBits[bitsOffset];
				if (((missingFamilyIndex >> indexOffset) & 1) > 0) {

					int vary = 1 << bits;

					for (int i = 0; i < vary; ++i) {
						getFamilies(families, familyCount, 
								(byte)(familyIndex	| (i << indexOffset)), missingFamilyIndex,
								(byte)(indexOffset + bits), familyFieldBits,
								++bitsOffset);
					}
				} else {

					getFamilies(families, familyCount, familyIndex,
							missingFamilyIndex, (byte)(indexOffset + bits),
							familyFieldBits, ++bitsOffset);
				}
			}
		}
*/
		byte[] getFamily(byte[] subj, byte[] pred, byte[] obj,
				byte[] ctx) {
			assert (subj != null && pred != null && obj != null && ctx != null);

			return getFamilies(subj, pred, obj, ctx)[0];
		}

		byte[] getQualifier(byte[] subj, byte[] pred, byte[] obj,
				byte[] ctx) {

			return getComposite(this.indexSpec.getQualifierFields(), true, true,
					subj, pred, obj, ctx);
		}
		
		public byte[] getValue(byte[] subj, byte[] pred, byte[] obj,
				byte[] ctx) {

			return getComposite(this.indexSpec.getValueFields(), true, true,
					subj, pred, obj, ctx);
		}

		Filter getFilter(final byte[] subj, final byte[] pred,
				final byte[] obj, final byte[] ctx) {

			final byte[][] filterFields = new byte[4][];

			boolean foundNull = false;

			char[] fieldSeq = this.indexSpec.getFieldSeq();
			for (int fieldNum = 0; fieldNum < 4; ++fieldNum) {
				char field = fieldSeq[fieldNum];
				switch (field) {
				case 's':
					filterFields[fieldNum] = subj;
					break;
				case 'p':
					filterFields[fieldNum] = pred;
					break;
				case 'o':
					filterFields[fieldNum] = obj;
					break;
				case 'c':
					filterFields[fieldNum] = ctx;
					break;
				default:
					throw new RuntimeException("invalid character '" + field
							+ "' in field sequence: " + new String(fieldSeq));
				}

				if (!foundNull) {
					if (filterFields[fieldNum] == null)
						foundNull = true;
					filterFields[fieldNum] = null;					
				} 
			}

			if (filterFields[0] != null || filterFields[1] != null || filterFields[2] != null || filterFields[3] != null) {
			
				return new StatementFilter(filterFields);	
			} else {
				return null;
			}
		}

		public byte[][] getValueIDs(KeyValue keyValue) {
			
			byte[][] valueIDs = new byte[4][ValueTable.NUM_VALUE_BYTES];

			char[] fieldSeq = this.indexSpec.getFieldSeq();
			char[] keyFields = this.indexSpec.getKeyFields();
			char[] qualifierFields = this.indexSpec.getQualifierFields();
			char[] valueFields = this.indexSpec.getValueFields();
			
			int valueIndex = -1;
			int offset = 0;
			for (int fieldNum = 0; fieldNum < 4; ++fieldNum) {
				
				char field = fieldSeq[fieldNum];
				switch (field) {
					case 's':
						valueIndex = 0;
						break;
					case 'p':
						valueIndex = 1;
						break;
					case 'o':
						valueIndex = 2;
						break;
					case 'c':
						valueIndex = 3;
						break;
					default:
						throw new RuntimeException("Invalid character '" + field
								+ "' in field sequence: " + new String(fieldSeq));
				}
				
				if (fieldNum == keyFields.length || fieldNum == (keyFields.length + qualifierFields.length))
					offset = 0;
								
				if (fieldNum < keyFields.length) {
					Bytes.putBytes(valueIDs[valueIndex], 0, keyValue.getBuffer(),
							keyValue.getRowOffset() + offset,
							valueIDs[valueIndex].length);
					offset += valueIDs[valueIndex].length;
				} else if (fieldNum < keyFields.length + qualifierFields.length) {
					Bytes.putBytes(valueIDs[valueIndex], 0, keyValue.getBuffer(),
							keyValue.getQualifierOffset() + offset,
							valueIDs[valueIndex].length);
					offset += valueIDs[valueIndex].length;
				} else if (fieldNum < keyFields.length + qualifierFields.length + valueFields.length) {
					Bytes.putBytes(valueIDs[valueIndex], 0, keyValue.getBuffer(),
							keyValue.getValueOffset() + offset,
							valueIDs[valueIndex].length);
					offset += valueIDs[valueIndex].length;
				}
			}

			return valueIDs;
		}
	}