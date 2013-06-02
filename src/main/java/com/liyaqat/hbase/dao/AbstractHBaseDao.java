package com.liyaqat.hbase.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

public abstract class AbstractHBaseDao implements   HBaseDao,InitializingBean {

	public AbstractHBaseDao() {
	}
	
	

	protected String tableName="EmployeeData";
	protected String EMPLID="emplid";
	protected String LAST="last";
	protected String MIDDLE="middle";
	protected String FIRST="first";
	protected String DISPLAY_NAME="displayname";
	
	protected String COLUMN_FAMILY="employee";
	
	@Autowired
	protected Configuration hbaseConfiguration;

	@Autowired
	protected HbaseTemplate hbaseTemplate;

	public void afterPropertiesSet() throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
		if (!admin.tableExists(tableName))
	 	{
			HTableDescriptor tableDes = new HTableDescriptor(tableName);
			HColumnDescriptor emplidColFm1 = new HColumnDescriptor(COLUMN_FAMILY);
			tableDes.addFamily(emplidColFm1);
			/*
			
			HColumnDescriptor firstColFm1 = new HColumnDescriptor(FIRST);
			tableDes.addFamily(firstColFm1);
			HColumnDescriptor lastColFm1 = new HColumnDescriptor(LAST);
			tableDes.addFamily(lastColFm1);
			HColumnDescriptor middleColFm1 = new HColumnDescriptor(MIDDLE);
			tableDes.addFamily(middleColFm1);
			HColumnDescriptor displayColFm1 = new HColumnDescriptor(DISPLAY_NAME);
			tableDes.addFamily(displayColFm1);
			*/
			admin.createTable(tableDes);
		}
	}

	
	
}
