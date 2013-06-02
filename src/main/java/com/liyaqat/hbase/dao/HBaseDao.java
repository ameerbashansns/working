package com.liyaqat.hbase.dao;

import java.util.List;

import com.liyaqat.hbase.domain.Employee;

public interface HBaseDao {
	
	public void addEmployee(Employee employee);
	public void updateEmployee(Employee employee);
	public void deleteEmployee(String emplid);
	public List<Employee> getEmployeeById(String emplid);
	public List<Employee> getByObjectName(String objectName);
	public void addBatch(List<Employee> batch);
}
