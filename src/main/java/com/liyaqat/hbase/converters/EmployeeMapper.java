package com.liyaqat.hbase.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.springframework.data.hadoop.hbase.RowMapper;

import com.liyaqat.hbase.domain.Employee;
import com.liyaqat.hbase.utils.HBaseDataUtil;

public class EmployeeMapper implements RowMapper<List<Employee>> {

	public EmployeeMapper() {
		// TODO Auto-generated constructor stub
	}
	public List<Employee> mapRow(Result result, int rowNum) throws Exception {
		
		List<Employee> employees= null;
		
		if(result != null && !result.isEmpty())
		{
			employees= new ArrayList<Employee>();
			HBaseDataUtil.convertResult(result, employees);
		}
		
		/*
		//System.out.println("Result  "+result);
		
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> data= result.getMap();
		if(data != null && !data.isEmpty())
		{
			employees=new ArrayList<Employee>();
			for(byte[] row: data.keySet())
			{
				//System.out.println(" Row - "+ new String(row));
				NavigableMap<byte[], NavigableMap<Long, byte[]>> columns= data.get(row);
				
				if(columns != null && !columns.isEmpty())
				{
					Employee employee= new Employee();
					
					for (byte[] column : columns.keySet())
					{
						String columnName=new String(column);
						String value="";
						for(Long time: columns.get(column).keySet())
						{
							value=new String(columns.get(column).get(time));
							//System.out.println(" value  - "+ value);
						}
						if(columnName.equalsIgnoreCase("displayname"))
						{
							employee.setDisplayname(value);
						}
						if(columnName.equalsIgnoreCase("emplid"))
						{
							employee.setEmplid(value);
						}
						if(columnName.equalsIgnoreCase("first"))
						{
							employee.setFirst(value);
						}
						if(columnName.equalsIgnoreCase("last"))
						{
							employee.setLast(value);
						}
						if(columnName.equalsIgnoreCase("Middle"))
						{
							employee.setMiddle(value);
						}
					}
					employees.add(employee);
				}
			}
		}*/
		return employees;
	}

}
