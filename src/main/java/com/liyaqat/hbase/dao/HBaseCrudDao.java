package com.liyaqat.hbase.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Component;

import com.liyaqat.hbase.converters.EmployeeExtractor;
import com.liyaqat.hbase.converters.EmployeeMapper;
import com.liyaqat.hbase.dao.exception.HBaseCrudException;
import com.liyaqat.hbase.domain.Employee;
import com.liyaqat.hbase.service.exception.HbaseCrudServiceException;

import org.apache.hadoop.conf.Configuration;
@Component
public class HBaseCrudDao extends AbstractHBaseDao 
{

	
	public HBaseCrudDao() {
	}


	
	
	public void addEmployee(final Employee employee)
	{

		hbaseTemplate.execute(tableName, new TableCallback<Object>()
		{
			public Object doInTable(HTableInterface table) throws Throwable 
			{
				hbaseTemplate.setAutoFlush(false);
				try
				{
					Put p = new Put(Bytes.toBytes(employee.getEmplid()));
					p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(FIRST), Bytes.toBytes(employee.getFirst()));	
					p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(LAST), Bytes.toBytes(employee.getLast()));
					p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(MIDDLE), Bytes.toBytes(employee.getMiddle()));
					p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(EMPLID), Bytes.toBytes(employee.getEmplid()));
					p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(DISPLAY_NAME), Bytes.toBytes(employee.getDisplayname()));
					table.put(p);		
				}
				catch(Exception e)
				{
					
					throw new HbaseCrudServiceException("The Employee "+employee.getEmplid()+" Create Error",e);
				}
				finally
				{
					hbaseTemplate.setAutoFlush(true);
				}
				return employee;
			}
		});
	}
	public void updateEmployee(final Employee employee)
	{
		hbaseTemplate.execute(tableName, new TableCallback<Object>()
		{
			public Object doInTable(HTableInterface table) throws Throwable 
			{
				hbaseTemplate.setAutoFlush(false);
				try
				{
					Put p = new Put(Bytes.toBytes(employee.getEmplid()));
					if(!p.has(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(FIRST), Bytes.toBytes(employee.getFirst())))
					{
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(FIRST), Bytes.toBytes(employee.getFirst()));	
					}
					if(!p.has(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(LAST), Bytes.toBytes(employee.getLast())))
					{
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(LAST), Bytes.toBytes(employee.getLast()));	
					}
					if(!p.has(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(MIDDLE), Bytes.toBytes(employee.getMiddle())))
					{
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(MIDDLE), Bytes.toBytes(employee.getMiddle()));	
					}
					if(!p.has(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(EMPLID), Bytes.toBytes(employee.getEmplid())))
					{
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(EMPLID), Bytes.toBytes(employee.getEmplid()));
					}
					if(!p.has(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(DISPLAY_NAME), Bytes.toBytes(employee.getDisplayname())))
					{
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(DISPLAY_NAME), Bytes.toBytes(employee.getDisplayname()));
					}
					if(!p.isEmpty())
					{
						table.put(p);		
					}
					else
					{
						System.out.println("The User "+employee.getEmplid()+" data is Identical");
					}
				}
				catch(Exception e)
				{
					throw new HbaseCrudServiceException("The Employee "+employee.getEmplid()+" Update Error",e);
				}
				finally
				{
					hbaseTemplate.setAutoFlush(true);
				}
				
				return employee;
			}
		});
		
	}
	public void deleteEmployee(final String emplid)
	{
		hbaseTemplate.execute(tableName, new TableCallback<Object>()
		{
			public Object doInTable(HTableInterface table) throws Throwable 
			{
				try
				{
					Delete delete = new Delete(Bytes.toBytes(emplid));
					table.delete(delete);
				}
				catch(Exception e)
				{
					throw new HbaseCrudServiceException("The Employee "+emplid+" Delete Error",e);
				}
				return null;
			}
		});
	}

	public List<Employee> getEmployeeById(String emplid)
	{
		List<Employee> data=hbaseTemplate.get(tableName,emplid, new EmployeeMapper());
		return data;
	}
	
	public List<Employee> getByObjectName(String objectName) {
		Scan scan= new Scan();
		scan.setCaching(1000);
		scan.setBatch(5);
		List<Employee> data=null;
		try
		{
			data=hbaseTemplate.find(objectName,scan, new EmployeeExtractor());
			System.out.println(data.size());	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		return data; 
			
	}




	@Override
	public void addBatch(final List<Employee> batch) {
		hbaseTemplate.execute(tableName, new TableCallback<Object>()
		{
			public Object doInTable(HTableInterface table) throws Throwable 
			{
				hbaseTemplate.setAutoFlush(false);
				try
				{
					List<Put> batchPuts= new ArrayList<Put>();
					for(Employee employee: batch)
					{
						Put p = new Put(Bytes.toBytes(employee.getEmplid()));
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(FIRST), Bytes.toBytes(employee.getFirst()));	
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(LAST), Bytes.toBytes(employee.getLast()));
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(MIDDLE), Bytes.toBytes(employee.getMiddle()));
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(EMPLID), Bytes.toBytes(employee.getEmplid()));
						p.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(DISPLAY_NAME), Bytes.toBytes(employee.getDisplayname()));
						batchPuts.add(p);
					}
					
					if(!batchPuts.isEmpty())
					{
						table.put(batchPuts);	
					}
				}
				catch(Exception e)
				{
					
					throw new HbaseCrudServiceException("The Employee Btach Process Error",e);
				}
				finally
				{
					hbaseTemplate.setAutoFlush(true);
					
				}
				return null;
			}
		});
		
	}

}
