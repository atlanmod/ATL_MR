package fr.inria.atlanmod.atl_mr.hbase.splitter;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage.Registry;
import org.eclipse.emf.ecore.EStructuralFeature;

import fr.inria.atlanmod.neoemf.util.NeoEMFUtil;
public abstract class AbstractSplitter {


	protected static final byte[] PROPERTY_FAMILY =				Bytes.toBytes("p");
	protected static final byte[] TYPE_FAMILY = 				Bytes.toBytes("t");
	protected static final byte[] METAMODEL_QUALIFIER =			Bytes.toBytes("m");
	protected static final byte[] ECLASS_QUALIFIER = 			Bytes.toBytes("e");
	protected static final byte[] CONTAINMENT_FAMILY = 			Bytes.toBytes("c");
	protected static final byte[] CONTAINER_QUALIFIER = 		Bytes.toBytes("n");
	protected static final byte[] CONTAINING_FEATURE_QUALIFIER = Bytes.toBytes("g");

	protected static final byte[] ORIGINAL_ID_FAMILY = Bytes.toBytes("of");
	protected static final byte[] ORIGINAL_ID_COLUMN = Bytes.toBytes("oc");

	protected HConnection tableConnection;
	protected HTable sourceTable;
	protected HTable saltedTable;
	protected URI tableURI;


	/**
	 * a footprints map organized by type
	 */
	protected HashMap<String, List<CallSite>> footprints;
	//protected Map<byte[], List<Dependency>> dependencyMap;
	protected Map<byte[], SimpleDepsList> dependencyMap;

	protected long modelSize;

	protected Scan scan;
	@SuppressWarnings("resource")
	public  AbstractSplitter (URI tableURI) {

		this.tableURI = tableURI;
		this.footprints = new HashMap<String, List<CallSite>>();
		dependencyMap = new HashMap<>();

		// setting up the custom scan
		this.scan = new Scan();
		// A filter to skip the root value for distribution
		FilterList filterList = new FilterList(new KeyOnlyFilter(),
				new RowFilter(CompareOp.NOT_EQUAL, new RegexStringComparator("ROOT")));
		{
			// 500 is the recommended for MR,
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			scan.setFilter(filterList);
			scan.setMaxVersions(1);
			scan.setBatch(1000);
		}

		// setting up the HBase tables
		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", tableURI.host());
		conf.set("hbase.zookeeper.property.clientPort", tableURI.port() != null ? tableURI.port() : "2181");

		// setting up the table name and row count
		TableName tableName = TableName.valueOf(NeoEMFUtil.formatURI(tableURI));
		HBaseAdmin admin;

		try {

			// computing row count
			modelSize  = new AggregationClient(conf).rowCount(sourceTable, new LongColumnInterpreter() , scan);
			// setting up tables
			admin = new HBaseAdmin(conf);
			URI saltedURI = tableURI.appendFragment("salted");
			//tableConnection =  HConnectionManager.createConnection(conf);

			if (!admin.tableExists(tableName)) {
				throw new IOException(MessageFormat.format(
						"Resource with URI {0} does not exist", tableName.getNameAsString()));
			}

			NeoEMFUtil.ResourceUtil.INSTANCE.deleteResourceIfExists(saltedURI);

			// Creating the salted table
			TableName saltedTableName = TableName.valueOf(NeoEMFUtil.formatURI(saltedURI));
			HTableDescriptor desc = new HTableDescriptor(saltedTableName);
			HColumnDescriptor origFamily = new HColumnDescriptor(ORIGINAL_ID_FAMILY);
			desc.addFamily(origFamily);
			admin.createTable(desc);

			sourceTable = new HTable(conf, tableName);
			saltedTable = new HTable(conf, saltedTableName);


			admin.close();


		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}



	public HTable getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(HTable sourceTable) {
		this.sourceTable = sourceTable;
	}

	public HTable getSaltedTable() {
		return saltedTable;
	}

	public void setSaltedTable(HTable saltedTable) {
		this.saltedTable = saltedTable;
	}

	public URI getTableURI() {
		return tableURI;
	}

	public void setTableURI(URI tableURI) {
		this.tableURI = tableURI;
	}


	protected boolean isHighLevel (byte[] objectId){
		return true;
	}

	protected EClass resolveInstanceOf(byte [] id) {
		try {
			Result result= sourceTable.get(new Get(id));
			String nsURI = Bytes.toString(result.getValue(TYPE_FAMILY, METAMODEL_QUALIFIER));
			String className = Bytes.toString(result.getValue(TYPE_FAMILY, ECLASS_QUALIFIER));
			if (nsURI != null && className != null) {
				EClass eClass = (EClass) Registry.INSTANCE.getEPackage(nsURI).getEClassifier(className);
				return eClass;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected String resolveInstanceNameOf(byte [] id) {
		Result result;
		try {
			result = sourceTable.get(new Get(id));
			return Bytes.toString(result.getValue(TYPE_FAMILY, ECLASS_QUALIFIER));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}




	abstract void split(int splits) throws Exception ;


	/**
	 *
	 * @author Amine BENELALLAM
	 *
	 */


	public class CallSite {

		EStructuralFeature feat;
		EClass receptor;

		public CallSite(EStructuralFeature feat, EClass receptor) {
			super();
			this.feat = feat;
			this.receptor = receptor;
		}
		public EStructuralFeature getFeature() {
			return feat;
		}
		public void setFeature(EStructuralFeature feat) {
			this.feat = feat;
		}
		public EClass getReceptor() {
			return receptor;
		}
		public void setReceptor(EClass receptor) {
			this.receptor = receptor;
		}
		@Override
		public String toString() {
			return " <" + receptor.getName() + "." + feat.getName() + ">";
		}

	}
	/**
	 *
	 * @author Amine BENELALLAM
	 *
	 */
	public class Dependency {

		int splitId;
		CallSite nextFP;

		public Dependency(int clusterId, CallSite nextFP) {
			super();
			this.splitId = clusterId;
			this.nextFP = nextFP;
		}
		public int getClusterId() {
			return splitId;
		}
		public void setClusterId(int clusterId) {
			this.splitId = clusterId;
		}
		public CallSite getNextFP() {
			return nextFP;
		}
		public void setNextFP(CallSite nextFP) {
			this.nextFP = nextFP;
		}


	}

	/**
	 *
	 * @author Amine BENELALLAM	 *
	 */
	public class SimpleDepsList {

		long [] dependecyCount;

		public SimpleDepsList(int numSplits) {
			dependecyCount = new long [numSplits];
		}

		public void updateDeps (int splitIndex) throws IndexOutOfBoundsException {
			dependecyCount [splitIndex] ++;
		}

		public long[] getDependecyCount() {
			return dependecyCount;
		}

		public void incrementSplit (int splitId) {
			dependecyCount[splitId] ++;
		}

	}
}
