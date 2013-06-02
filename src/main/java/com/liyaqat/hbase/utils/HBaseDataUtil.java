package com.liyaqat.hbase.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.liyaqat.hbase.domain.Employee;

public class HBaseDataUtil {

	private HBaseDataUtil() {
		// TODO Auto-generated constructor stub
	}
	
	public static void convertResult(Result result,List<Employee> employees)
	{
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> data= result.getMap();
		if(data != null && !data.isEmpty())
		{
			for(byte[] row: data.keySet())
			{
				//System.out.println(" Row - "+ new String(row));
				NavigableMap<byte[], NavigableMap<Long, byte[]>> columns= data.get(row);
				if(columns != null && !columns.isEmpty())
				{
					Employee employee= new Employee();
					employee.setDisplayname(getString(columns,"displayname"));
					employee.setFirst(getString(columns,"first"));
					employee.setLast(getString(columns,"last"));
					employee.setEmplid(getString(columns,"emplid"));
					employee.setMiddle(getString(columns,"middle"));
					employees.add(employee);
				}
			}
		}

	}

	private static String getString(NavigableMap<byte[], NavigableMap<Long, byte[]>> columns,String key)
	{
		String result="";
		result=new String(columns.get(Bytes.toBytes(key)).firstEntry().getValue());
		//System.out.println(String.format("%s-%s",key, result));
		return result;
	}

}
