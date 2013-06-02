package com.liyaqat.hbase.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseUtils;
import org.springframework.data.hadoop.hbase.ResultsExtractor;

import com.liyaqat.hbase.domain.Employee;
import com.liyaqat.hbase.utils.HBaseDataUtil;

public class EmployeeExtractor implements ResultsExtractor<List<Employee>> {

	public EmployeeExtractor() {
	}

	public List<Employee> extractData(ResultScanner results) throws Exception {
		List<Employee> employees= null;
		if(results != null)
		{
			employees=new ArrayList<Employee>();
			for(Result result: results)
			{
				HBaseDataUtil.convertResult(result, employees);
			}
		}
		return employees;
	}
}
