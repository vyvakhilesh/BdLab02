package com.hbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class HbaseStart
{
	static public void main(String args[]) throws IOException {
		
	  //  createTable();
	//	insertTable();
	//	retrieveTable();
	//	deleteTable();
	//	getAllRow();
		getAllTable();
		}
	
	
	public static void createTable() throws IOException
	{
		Configuration config = HBaseConfiguration.create();		 
		config.clear();
         config.set("hbase.zookeeper.quorum", "134.193.136.127");
         config.set("hbase.zookeeper.property.clientPort","2181");
         config.set("hbase.master", "134.193.136.127:60010");
		
		HBaseAdmin admin = new HBaseAdmin(config);
		
		try {
			// HBaseConfiguration hc = new HBaseConfiguration(new Configuration());
			
			  HTableDescriptor ht = new HTableDescriptor("AkileshTable"); 
			  
			  ht.addFamily( new HColumnDescriptor("Location"));

		//	  ht.addFamily( new HColumnDescriptor("longitude"));
			  
			  ht.addFamily( new HColumnDescriptor("Date"));
			  
			  ht.addFamily( new HColumnDescriptor("Dimensions"));
			  
			  ht.addFamily( new HColumnDescriptor("Humidity"));
			  
			  ht.addFamily( new HColumnDescriptor("Temperature"));
			  
			  System.out.println( "connecting" );

			  HBaseAdmin hba = new HBaseAdmin( config );

			  System.out.println( "Creating Table" );

			  hba.createTable( ht );

			  System.out.println("Done......");
			  hba.close();
			  	
        } finally {
            admin.close();
        }
		
		
	}
	
	
	public static void insertTable() throws IOException{
	
		Configuration config = HBaseConfiguration.create();		 
		config.clear();
         config.set("hbase.zookeeper.quorum", "134.193.136.127");
         config.set("hbase.zookeeper.property.clientPort","2181");
         config.set("hbase.master", "134.193.136.127:60010");
         
         
         
         System.out.println("Check here ");

		  HTable table = new HTable(config, "AkileshTable");
	
		  //Put p = new Put(Bytes.toBytes("row1"));
		  
		  int count=1;
		  int timestamp=10000;
         
        BufferedReader br = null;
         
 		try {
  
 			String sCurrentLine;
 			String latitude="",longitude="",Date="",x="",y="",z="",humid="",temp="";
 			br = new BufferedReader(new FileReader("C:/Users/tirumala/Desktop/desktop/Assignments and Homeworks/bigdata/lab 02//sens.txt"));
 			
 			while ((sCurrentLine = br.readLine()) != null) {
 				
 				 Put p = new Put(Bytes.toBytes("row1"),timestamp);
 				
 				if(sCurrentLine.equals(""))
 				{
 					continue;
 				}
 				System.out.println("\nCurrent Line :" + sCurrentLine);
 				String[] array = sCurrentLine.split("\t");
 				String delim="#";
 				int check=0;
 				while(array[check].contains("*")==false)
 				{
 					System.out.println("\nInside while :" + array[check]+"\t" +check);
 					String[] words=array[check].split(delim);
 					if (words[0].contains("Latitude"))
 						latitude = words[1];
 					if (words[0].contains("Longitude"))
 						longitude=words[1];
 					if (words[0].contains("Date"))
 						Date=words[1];
 					if (words[0].contains("X"))
 						x=words[1];
 					if (words[0].contains("Y"))
 						y=words[1];
 					if (words[0].contains("Z"))
 						z=words[1];
 					if (words[0].contains("Humidity"))
 						humid = words[1];
 					if (words[0].contains("Hum Ambient temperature"))
 						temp=words[1];
 					check++;
 					System.out.println("Date : "+Date);
 					System.out.println("Latitude : "+latitude);
 					System.out.println("Longitude : "+longitude);
 				//	System.out.println("Date : "+x);
 				
 				}
 				
 				if (latitude!="" || longitude!="")
 				{
 					p.add(Bytes.toBytes("Location"), Bytes.toBytes("col"+count),Bytes.toBytes(latitude));
 					p.add(Bytes.toBytes("Location"), Bytes.toBytes("col"+(count+1)),Bytes.toBytes(longitude));
 					System.out.println(" Inside if condition Latitude : "+latitude);
 				} 
 				// p.add(Bytes.toBytes("longitude"), Bytes.toBytes("col"+(count+1)),Bytes.toBytes(longitude));
 				if (Date!="")
 				 p.add(Bytes.toBytes("Date"), Bytes.toBytes("col"+(count+2)),Bytes.toBytes(Date));
 				//if (x!="" || y!="" || z!="")
 				 p.add(Bytes.toBytes("Dimensions"), Bytes.toBytes("col"+(count+3)),Bytes.toBytes(x));
 				p.add(Bytes.toBytes("Dimensions"), Bytes.toBytes("col"+(count+4)),Bytes.toBytes(y));
 				p.add(Bytes.toBytes("Dimensions"), Bytes.toBytes("col"+(count+5)),Bytes.toBytes(z));
 				 if (humid !="")
 					 p.add(Bytes.toBytes("Humidity"), Bytes.toBytes("col"+(count+6)),Bytes.toBytes(humid));
 				 if (temp!="")
 					 p.add(Bytes.toBytes("Temperature"), Bytes.toBytes("col"+(count+7)),Bytes.toBytes(temp));
 			//	p.add(Bytes.toBytes("y"), Bytes.toBytes("col"+(count+4)),Bytes.toBytes(y));
 				
 			//	p.add(Bytes.toBytes("z"), Bytes.toBytes("col"+(count+5)),Bytes.toBytes(z));
 				 
 			      table.put(p);
 			     // check++;
 			      count=count+8;
 			      timestamp=timestamp+1;
 				
 			}
 			System.out.println("Insert Successfully");
  
 		} catch (IOException e) {
 			e.printStackTrace();
 		} finally {
 			try {
 				if (br != null)br.close();
 			} catch (IOException ex) {
 				ex.printStackTrace();
 			}
 		}
         
         
		
		
	  
	    
	}
	
	
	public static void retrieveTable() throws IOException{
		
		Configuration config = HBaseConfiguration.create();		 
		config.clear();
         config.set("hbase.zookeeper.quorum", "134.193.136.127");
         config.set("hbase.zookeeper.property.clientPort","2181");
         config.set("hbase.master", "134.193.136.127:60010");
		
		
		  HTable table = new HTable(config, "AkileshTable");
		
		 Get g = new Get(Bytes.toBytes("row1"));

		  Result r = table.get(g);
		  
		  for (int display=0;display<=800;)
		  {

		  byte [] value = r.getValue(Bytes.toBytes("Location"),Bytes.toBytes("col"+(display+1)));

		  byte [] value1 = r.getValue(Bytes.toBytes("Location"),Bytes.toBytes("col"+(display+2)));

		  byte [] value2 = r.getValue(Bytes.toBytes("Date"),Bytes.toBytes("col"+(display+3)));
		  
		  byte [] value3 = r.getValue(Bytes.toBytes("Dimensions"),Bytes.toBytes("col"+(display+4)));
		  
		  byte [] value4 = r.getValue(Bytes.toBytes("Dimensions"),Bytes.toBytes("col"+(display+5)));
		  
		  byte [] value5 = r.getValue(Bytes.toBytes("Dimensions"),Bytes.toBytes("col"+(display+6)));
		  
		  byte [] value6 = r.getValue(Bytes.toBytes("Humidity"),Bytes.toBytes("col"+(display+7)));
		  
		  byte [] value7 = r.getValue(Bytes.toBytes("Temperature"),Bytes.toBytes("col"+(display+8)));
		  
		  String valueStr = Bytes.toString(value);

		  String valueStr1 = Bytes.toString(value1);
		  
		  String valueStr2 = Bytes.toString(value2);
		  
		  String valueStr3 = Bytes.toString(value3);
		  
		  String valueStr4 = Bytes.toString(value4);
		  
		  String valueStr5 = Bytes.toString(value5);

		  String valueStr6 = Bytes.toString(value6);
		  
		  String valueStr7 = Bytes.toString(value7);
		  
		  
		  System.out.println("\nGET: " +"latitude: "+ valueStr+"longitude: "+valueStr1);
		  System.out.println("GET: " +"Date: "+ valueStr2);
		  System.out.println("GET: " +"x: "+ valueStr3);
		  System.out.println("GET: " +"y: "+ valueStr4);
		  System.out.println("GET: " +"z: "+ valueStr5);
		  System.out.println("GET: " +"Humidity: "+ valueStr6+"Temperature: "+valueStr7);
		  display+=8;

		  }

		/*  Scan s = new Scan();

		  s.addColumn(Bytes.toBytes("latitude"), Bytes.toBytes("col1"));

		  s.addColumn(Bytes.toBytes("longitude"), Bytes.toBytes("col2"));

		  ResultScanner scanner = table.getScanner(s);

		  try
		  {
		   for (Result rr = scanner.next(); rr != null; rr = scanner.next())
		   {
		    System.out.println("Found row : " + rr);
		   }
		  } finally
		  {
		   // Make sure you close your scanners when you are done!
		   scanner.close();
		  }
		*/
	}
	
	
	public static void deleteTable() throws IOException{
		
		Configuration config = HBaseConfiguration.create();		 
		config.clear();
         config.set("hbase.zookeeper.quorum", "134.193.136.127");
         config.set("hbase.zookeeper.property.clientPort","2181");
         config.set("hbase.master", "134.193.136.127:60010");
         
         HBaseAdmin admin = new HBaseAdmin(config);
         admin.disableTable("AkileshTable");
         admin.deleteTable("AkileshTable");

	}
	
	public static void getAllRow() throws IOException
	{
		Configuration config = HBaseConfiguration.create();		 
		config.clear();
         config.set("hbase.zookeeper.quorum", "134.193.136.127");
         config.set("hbase.zookeeper.property.clientPort","2181");
         config.set("hbase.master", "134.193.136.127:60010");
         
         HTable table = new HTable(config, "AkileshTable");
         Get g = new Get(Bytes.toBytes("row1"));

         BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("retrieverow.txt"), "utf-8"));
         
		  Result r = table.get(g);
		  int check_here=0;
         for(KeyValue kv : r.raw()){
        	 
             writer.write("\n"+new String(kv.getRow()) + " Get All Row " );
             writer.write(new String(kv.getFamily()) + ":" );
             writer.write(new String(kv.getQualifier()) + " " );
             writer.write(kv.getTimestamp() + " " );
             writer.write(new String(kv.getValue()));
          /*   check_here++;
             if (check_here>=100)
            	 break;*/
         }
         
   /*      for(KeyValue kv : r.raw()){
        	 
        	 String familyname = new String(kv.getFamily());
            
        	 if(familyname.equals("Accelerometer"))
        	 {
        		 System.out.println("=============="+familyname+"==============");
        		 System.out.print(new String(kv.getQualifier())+":");
        		 System.out.println(new String(kv.getValue()));
        	 }
        	 
         }*/
	}
	
	public static void getAllTable() throws IOException
	{
		Configuration config = HBaseConfiguration.create();		 
		config.clear();
         config.set("hbase.zookeeper.quorum", "134.193.136.127");
         config.set("hbase.zookeeper.property.clientPort","2181");
         config.set("hbase.master", "134.193.136.127:60010");
		
		try{
            HTable table = new HTable(config, "AkileshTable");
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            /* BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("retrievetable.txt"), "utf-8"));
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                   writer.write("\n"+new String(kv.getRow()) + " GeT ALL Akilesh TABLE ");
                   writer.write(new String(kv.getFamily()) + ":");
                   writer.write(new String(kv.getQualifier()) + " ");
                   writer.write(kv.getTimestamp() + " ");
                   writer.write(new String(kv.getValue()));
                }
            }*/
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                   System.out.println("\n"+new String(kv.getRow()) + " GeT ALL Akilesh TABLE ");
                   System.out.println(new String(kv.getFamily()) + ":");
                   System.out.println(new String(kv.getQualifier()) + " ");
                   System.out.println(kv.getTimestamp() + " ");
                   System.out.println(new String(kv.getValue()));
                }
            }
       } catch (IOException e){
           e.printStackTrace();
       }
		
	}
}

