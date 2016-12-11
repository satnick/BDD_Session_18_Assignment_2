package hbasePract.tableOperation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class CompanyTableOp {
	Configuration conf=HBaseConfiguration.create();
		
	public void createtable(String name,String[] colfamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		
		
		HBaseAdmin admin=new HBaseAdmin(conf);
		HTableDescriptor des=new HTableDescriptor(Bytes.toBytes(name));
		
		for (int i = 0; i < colfamily.length; i++) {
			des.addFamily(new HColumnDescriptor(colfamily[i]));
        }

		
		
		if(admin.tableExists(name)){
		
	System.out.println("Table Already exists");
		}
		else{
			
			admin.createTable(des);
			
		System.out.println("Table:"+ name +" sucessfully created");
		}
		
	}
	public   void insert(String tableName, String rowKey,String family, String qualifier, String value)
																					throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

public void getrecord(String tablename,String rowkey) throws IOException{
	HTable table=new HTable(conf, tablename);
	Get get=new Get(Bytes.toBytes(rowkey));
	Result rs=table.get(get);
	for(KeyValue kv : rs.raw()){
        System.out.print(new String(kv.getRow()) + " " );
        System.out.print(new String(kv.getFamily()) + ":" );
        System.out.print(new String(kv.getQualifier()) + " " );
        System.out.print(kv.getTimestamp() + " " );
        System.out.println(new String(kv.getValue()));
    }
}
public void display(String tablename) throws IOException{
	HTable table=new HTable(conf,tablename);
	Scan sc=new Scan();
	ResultScanner rs=table.getScanner(sc);
	for(Result r:rs){
        for(KeyValue kv : r.raw()){
           System.out.print(new String(kv.getRow()) + " ");
           System.out.print(new String(kv.getFamily()) + ":");
           System.out.print(new String(kv.getQualifier()) + " ");
           System.out.print(kv.getTimestamp() + " ");
           System.out.println(new String(kv.getValue()));
        }
	
}
}
public static void main(String[] args) throws  Exception {
	
	CompanyTableOp op=new CompanyTableOp();
	String tablename = "company";
    String[] familys = { "name" };
    op.createtable(tablename, familys);
	
   

  //first record
    op.insert(tablename, "n1", "name", "firstName", "satish");
    op.insert(tablename, "n1", "name", "lastName", "kumar");
    
    //second record
    op.insert(tablename, "n2", "name", "firstName", "Rahul");
    op.insert(tablename, "n2", "name", "middleName", "Raj");
    op.insert(tablename, "n2", "name", "lastName", "Sharma");
	
	op.insert(tablename, "n3", "name", "firstName", "Amy");
    op.insert(tablename, "n3", "name", "middleName", "Rose");
    op.insert(tablename, "n3", "name", "lastName", "Antony");
    
	//retrieve one record
   op.getrecord("company", "n1");
    op.getrecord("company", "n2");
   
   //retrieve all records
   op.display(tableName);

	
}
}


