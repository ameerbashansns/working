package com.liyaqat.hbase.service;

import java.util.List;

import com.liyaqat.hbase.domain.Employee;

public interface HBaseCrudService 
{
	public void addEmployee(Employee employee);
	public void updateEmployee(Employee employee);
	public void deleteEmployee(String emplid);
	public List<Employee> getEmployeeById(String emplid);
	public List<Employee> getByObjectName(String objectName);
	public void addBatch(List<Employee> batch) ;
}
