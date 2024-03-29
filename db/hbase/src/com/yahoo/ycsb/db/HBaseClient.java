/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.db;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
//import java.util.HashMap;
//import java.util.Properties;
//import java.util.Set;
//import java.util.Vector;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.DistributedHBaseCluster;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseClusterManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
//import org.apache.hadoop.hbase.io.Cell;
//import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

/**
 * HBase client for YCSB framework
 */
@SuppressWarnings("deprecation")
public class HBaseClient extends com.yahoo.ycsb.DB
{
    private static final Configuration config = HBaseConfiguration.create();

    public boolean _debug=false;

    public String _table="";
    public HTable _hTable=null;
    public String _columnFamily="";
    public byte _columnFamilyBytes[];

    public static final int Ok=0;
    public static final int ServerError=-1;
    public static final int HttpError=-2;
    public static final int NoMatchingRecord=-3;

    public static final Object tableLock = new Object();

    private static Thread rollingRestarter;

    private static final String HBASE_CLUSTER_MANAGER_CLASS = "hbase.it.clustermanager.class";
    private static final Class<? extends ClusterManager> DEFAULT_HBASE_CLUSTER_MANAGER_CLASS = 
      HBaseClusterManager.class;

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		if ( (getProperties().getProperty("debug")!=null) &&
				(getProperties().getProperty("debug").compareTo("true")==0) )
		{
		    _debug=true;
	    }

	    _columnFamily = getProperties().getProperty("columnfamily");
	    if (_columnFamily == null) 
	    {
		    System.err.println("Error, must specify a columnfamily for HBase table");
		    throw new DBException("No columnfamily specified");
	    }
      _columnFamilyBytes = Bytes.toBytes(_columnFamily);

      if (getProperties().getProperty("rollingrestart.initsleep") == null) {
        return; // No more further initialization
      }

      synchronized (HBaseClient.class) {
        if (rollingRestarter == null) {
          rollingRestarter = startRollingRestarter();
        }
      }
	  }

    private Thread startRollingRestarter() {
      // Restart region servers one by one periodically
      Thread rollingRestarter = new Thread() {
        HBaseCluster cluster;
        int currentServer;
        ServerName master;

        @Override
        public void run() {
          try {
            System.out.println("Rolling restarter started");
            Class<? extends ClusterManager> clusterManagerClass = config.getClass(
              HBASE_CLUSTER_MANAGER_CLASS,
              DEFAULT_HBASE_CLUSTER_MANAGER_CLASS, ClusterManager.class);
            ClusterManager clusterManager = ReflectionUtils.newInstance(
              clusterManagerClass, config);
            cluster = new DistributedHBaseCluster(config, clusterManager);
            String tmp = getProperties().getProperty("rollingrestart.initsleep");
            long time = Long.parseLong(tmp) * 1000;
            Thread.sleep(time);
            tmp = getProperties().getProperty("rollingrestart.excludemaster");
            boolean excludeMaster = "true".equalsIgnoreCase(tmp);
            tmp = getProperties().getProperty("rollingrestart.sleep");
            time = Long.parseLong(tmp) * 1000;
            while (true) {
              Thread.sleep(time);
              try {
                restart(getNextServer(excludeMaster));
              } catch (IOException ioe) {
                System.out.println("Failed to restart a server due to " + ioe.getMessage());
                ioe.printStackTrace(System.err);
              }
            }
          } catch (Throwable t) {
            t.printStackTrace();
          }
          System.out.println("Rolling restarter exited");
        }

        ServerName getNextServer(boolean excludeMaster) throws IOException {
          ClusterStatus clusterStatus = cluster.getClusterStatus();
          Collection<ServerName> regionServers = clusterStatus.getServers();
          if (regionServers == null) {
            return null;
          }
          master = clusterStatus.getMaster();
          ArrayList<ServerName> tmp =
            new ArrayList<ServerName>(regionServers);
          if (excludeMaster) {
            tmp.remove(master);
          } else if (!tmp.contains(master)) {
            tmp.add(master);
          }
          if (tmp.isEmpty()) {
            return null;
          }
          Collections.sort(tmp); // Sort it so that we use fixed order
          currentServer = (currentServer + 1) % tmp.size();
          ServerName server = tmp.get(currentServer);
          if (server.equals(master)) {
            return master;
          }
          // Return the region server with most of regions.
          // Otherwise, all regions may stick with one server
          // that not restarted for a while
          int regionCount = -1;
          List<HRegionInfo> regions;
          for (ServerName sn: tmp) {
            if (sn.equals(master)) {
              continue;
            }
            try {
              regions = ProtobufUtil.getOnlineRegions(
                cluster.getAdminProtocol(sn));
            } catch (IOException ie) {
              // Can't talk to this region server
              continue;
            }
            if (regions == null || regions.isEmpty()) {
              continue;
            }
            int count = regions.size();
            if (count > regionCount) {
              regionCount = count;
              server = sn;
            }
          }
          return server;
        }

        void restart(ServerName server) throws IOException {
          if (server == null) {
            System.out.println("Skipped restarting null server");
            return;
          }

          if (server.equals(master)) {
            restartMaster(server);
          } else {
            restartRegionServer(server);
          }
        }

        void restartMaster(ServerName server) throws IOException {
          System.out.println("Killing master: " + server);
          cluster.killMaster(server);
          cluster.waitForMasterToStop(server, 60000);
          System.out.println("Killed master: " + server);

          System.out.println("Starting master: " + server.getHostname());
          cluster.startMaster(server.getHostname());
          cluster.waitForActiveAndReadyMaster(60000);
          System.out.println("Started master: " + server);
        }

        void restartRegionServer(ServerName server) throws IOException {
          System.out.println("Killing region server: " + server);
          cluster.killRegionServer(server);
          cluster.waitForRegionServerToStop(server, 60000);
          System.out.println("Killed region server: "
            + server + ". Reported num of rs: "
            + cluster.getClusterStatus().getServersSize());

          System.out.println("Starting region server: " + server.getHostname());
          cluster.startRegionServer(server.getHostname());
          cluster.waitForRegionServerToStart(server.getHostname(), 60000);
          System.out.println("Started region server: "
            + server + ". Reported num of rs: "
            + cluster.getClusterStatus().getServersSize());
        }
      };
      rollingRestarter.setDaemon(true);
      rollingRestarter.start();
      return rollingRestarter;
    }

    /**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
    public void cleanup() throws DBException
    {
        try {
            if (_hTable != null) {
                _hTable.flushCommits();
            }
        } catch (IOException e) {
            throw new DBException(e);
        }
    }

    public void getHTable(String table) throws IOException
    {
        synchronized (tableLock) {
            _hTable = new HTable(config, table);
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try 
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        Result r = null;
        try
        {
	    if (_debug) {
		System.out.println("Doing read from HBase columnfamily "+_columnFamily);
		System.out.println("Doing read for key: "+key);
	    }
            Get g = new Get(Bytes.toBytes(key));
          if (fields == null) {
            g.addFamily(_columnFamilyBytes);
          } else {
            for (String field : fields) {
              g.addColumn(_columnFamilyBytes, Bytes.toBytes(field));
            }
          }
            r = _hTable.get(g);
        }
        catch (IOException e)
        {
            System.err.println("Error doing get: "+e);
            return ServerError;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...need to understand HBase concurrency model better
            return ServerError;
        }

  for (KeyValue kv : r.raw()) {
    result.put(
        Bytes.toString(kv.getQualifier()),
        new ByteArrayByteIterator(kv.getValue()));
    if (_debug) {
      System.out.println("Result for field: "+Bytes.toString(kv.getQualifier())+
          " is: "+Bytes.toString(kv.getValue()));
    }

  }
	return Ok;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try 
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        Scan s = new Scan(Bytes.toBytes(startkey));
        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
        //We get back recordcount records
        s.setCaching(recordcount);

        //add specified fields or else all fields
        if (fields == null)
        {
            s.addFamily(_columnFamilyBytes);
        }
        else
        {
            for (String field : fields)
            {
                s.addColumn(_columnFamilyBytes,Bytes.toBytes(field));
            }
        }

        //get results
        ResultScanner scanner = null;
        try {
            scanner = _hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                //get row key
                String key = Bytes.toString(rr.getRow());
                if (_debug)
                {
                    System.out.println("Got scan result for key: "+key);
                }

                HashMap<String,ByteIterator> rowResult = new HashMap<String, ByteIterator>();

                for (KeyValue kv : rr.raw()) {
                  rowResult.put(
                      Bytes.toString(kv.getQualifier()),
                      new ByteArrayByteIterator(kv.getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult);
                numResults++;
                if (numResults >= recordcount) //if hit recordcount, bail out
                {
                    break;
                }
            } //done with row

        }

        catch (IOException e) {
            if (_debug)
            {
                System.out.println("Error in getting/parsing scan result: "+e);
            }
            return ServerError;
        }

        finally {
            scanner.close();
        }

        return Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String,ByteIterator> values)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try 
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }


        if (_debug) {
            System.out.println("Setting up put for key: "+key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> entry : values.entrySet())
        {
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/"+
                  entry.getValue() + " to put request");
            }	       
            p.add(_columnFamilyBytes,Bytes.toBytes(entry.getKey()),entry.getValue().toArray());
        }

        try 
        {
            _hTable.put(p);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing put: "+e);
            }
            return ServerError;
        }
        catch (ConcurrentModificationException e) 
        {
            //do nothing for now...hope this is rare
            return ServerError;
        }

        return Ok;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key, HashMap<String,ByteIterator> values)
    {
        return update(table,key,values);
    }

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try 
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        if (_debug) {
            System.out.println("Doing delete for key: "+key);
        }

        Delete d = new Delete(Bytes.toBytes(key));
        try 
        {
            _hTable.delete(d);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing delete: "+e);
            }
            return ServerError;
        }

        return Ok;
    }

    public static void main(String[] args)
    {
        if (args.length!=3)
        {
            System.out.println("Please specify a threadcount, columnfamily and operation count");
            System.exit(0);
        }

        final int keyspace=10000; //120000000;

        final int threadcount=Integer.parseInt(args[0]);	 

        final String columnfamily=args[1];


        final int opcount=Integer.parseInt(args[2])/threadcount;

        Vector<Thread> allthreads=new Vector<Thread>();

        for (int i=0; i<threadcount; i++)
        {
            Thread t=new Thread() 
            {
                public void run()
                {
                    try
                    {
                        Random random=new Random();

                        HBaseClient cli=new HBaseClient();

                        Properties props=new Properties();
                        props.setProperty("columnfamily",columnfamily);
                        props.setProperty("debug","true");
                        cli.setProperties(props);

                        cli.init();

                        //HashMap<String,String> result=new HashMap<String,String>();

                        long accum=0;

                        for (int i=0; i<opcount; i++)
                        {
                            int keynum=random.nextInt(keyspace);
                            String key="user"+keynum;
                            long st=System.currentTimeMillis();
                            int rescode;
                            /*
                            HashMap hm = new HashMap();
                            hm.put("field1","value1");
                            hm.put("field2","value2");
                            hm.put("field3","value3");
                            rescode=cli.insert("table1",key,hm);
                            HashSet<String> s = new HashSet();
                            s.add("field1");
                            s.add("field2");
                            
                            rescode=cli.read("table1", key, s, result);
                            //rescode=cli.delete("table1",key);
                            rescode=cli.read("table1", key, s, result);
                            */
                            HashSet<String> scanFields = new HashSet<String>();
                            scanFields.add("field1");
                            scanFields.add("field3");
                            Vector<HashMap<String,ByteIterator>> scanResults = new Vector<HashMap<String,ByteIterator>>();
                            rescode = cli.scan("table1","user2",20,null,scanResults);
                           
                            long en=System.currentTimeMillis();

                            accum+=(en-st);

                            if (rescode!=Ok)
                            {
                                System.out.println("Error "+rescode+" for "+key);
                            }

                            if (i%1==0)
                            {
                                System.out.println(i+" operations, average latency: "+(((double)accum)/((double)i)));
                            }
                        }

                        //System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
                        //System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            };
            allthreads.add(t);
        }

        long st=System.currentTimeMillis();
        for (Thread t: allthreads)
        {
            t.start();
        }

        for (Thread t: allthreads)
        {
            try
            {
                t.join();
            }
            catch (InterruptedException e)
            {
            }
        }
        long en=System.currentTimeMillis();

        System.out.println("Throughput: "+((1000.0)*(((double)(opcount*threadcount))/((double)(en-st))))+" ops/sec");

    }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/

