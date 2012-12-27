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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

/**
 * HBase Thrift client for YCSB framework
 */
public class ThriftClient extends com.yahoo.ycsb.DB
{
    private static final Configuration config = HBaseConfiguration.create();

    public boolean _debug=false;

    public String _table="";
    public ByteBuffer _tableName=null;
    public Client _client=null;
    public TTransport _transport=null;
    public String _columnFamily="";
    public byte _columnFamilyBytes[];
    public List<ByteBuffer> _columns;

    public static final int Ok=0;
    public static final int ServerError=-1;
    public static final int HttpError=-2;
    public static final int NoMatchingRecord=-3;

    public static final Object tableLock = new Object();

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
      _columns = new ArrayList<ByteBuffer>();
      _columns.add(ByteBuffer.wrap(_columnFamilyBytes));

    }

    /**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
    public void cleanup() throws DBException
    {
        try {
            if (_transport != null) {
              _transport.close();
            }
        } catch (Throwable e) {
            throw new DBException(e);
        }
    }

    public void getHTable(String table) throws IOException
    {
        synchronized (tableLock) {
          if (_transport != null) {
            try {
              _transport.close();
            } catch (Throwable t) {
              throw new IOException(t);
            }
          }

          String host = config.get("thrift.hostname", "localhost");
          int port = config.getInt("thrift.port", 9090);
          _transport = new TSocket(host, port);
          TProtocol protocol = new TBinaryProtocol(_transport, true, true);
          _client = new Hbase.Client(protocol);

          _tableName = ByteBuffer.wrap(Bytes.toBytes(table));
          try {
            _transport.open();
          } catch (TTransportException tte) {
            throw new IOException(tte);
          }
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
            _tableName = null;
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

        List<TRowResult> r = null;
        try
        {
	    if (_debug) {
		System.out.println("Doing read from HBase columnfamily "+_columnFamily);
		System.out.println("Doing read for key: "+key);
	    }
          r = _client.getRowWithColumns(_tableName, ByteBuffer.wrap(Bytes.toBytes(key)), _columns, null);
        }
        catch (Throwable e)
        {
            System.err.println("Error doing get: "+e);
            return ServerError;
        }

  for (TRowResult rowResult : r) {
    for (Map.Entry<ByteBuffer, TCell> column : rowResult.columns.entrySet()) {
      result.put(
          Bytes.toString(column.getKey().array()),
          new ByteArrayByteIterator(column.getValue().getValue()));
      if (_debug) {
        System.out.println("Result for field: "+Bytes.toString(column.getKey().array())+
            " is: "+Bytes.toString(column.getValue().getValue()));
      }
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
          _tableName = null;
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

        //get results
        int scanner = -1;
        try {
            scanner = _client.scannerOpen(_tableName, ByteBuffer.wrap(Bytes.toBytes(startkey)), _columns, null);
            int numResults = 0;
            for (List<TRowResult> rr = _client.scannerGet(scanner); rr != null && !rr.isEmpty(); rr = _client.scannerGet(scanner))
            {
              for (TRowResult rowResult: rr) {
                //get row key
                String key = Bytes.toString(rowResult.getRow());
                if (_debug)
                {
                    System.out.println("Got scan result for key: "+key);
                }

                HashMap<String,ByteIterator> rowResult2 = new HashMap<String, ByteIterator>();

                for (Map.Entry<ByteBuffer, TCell> column : rowResult.columns.entrySet()) {
                  rowResult2.put(
                      Bytes.toString(column.getKey().array()),
                      new ByteArrayByteIterator(column.getValue().getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult2);
                numResults++;
                if (numResults >= recordcount) //if hit recordcount, bail out
                {
                    break;
                }
              }
              if (numResults >= recordcount) //if hit recordcount, bail out
              {
                  break;
              }
            } //done with row

        }

        catch (Throwable e) {
            if (_debug)
            {
                System.out.println("Error in getting/parsing scan result: "+e);
            }
            return ServerError;
        }

        finally {
          try {
            _client.scannerClose(scanner);
          } catch (Throwable t) {
            // ignore
          }
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
          _tableName = null;
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
        ArrayList<Mutation> mutations = new ArrayList<Mutation>();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet())
        {
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/"+
                  entry.getValue() + " to put request");
            }	
            mutations.add(new Mutation(false,
              ByteBuffer.wrap(Bytes.toBytes(_columnFamily +":" + entry.getKey())),
              ByteBuffer.wrap(entry.getValue().toArray()), true));
        }

        try
        {
            _client.mutateRow(_tableName, ByteBuffer.wrap(Bytes.toBytes(key)), mutations, null);
        }
        catch (Throwable e)
        {
            if (_debug) {
                System.err.println("Error doing put: "+e);
            }
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
          _tableName = null;
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

        try
        {
            _client.deleteAllRow(_tableName, ByteBuffer.wrap(Bytes.toBytes(key)), null);
        }
        catch (Throwable e)
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

