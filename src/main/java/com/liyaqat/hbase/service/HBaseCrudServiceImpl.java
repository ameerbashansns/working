package com.liyaqat.hbase.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.liyaqat.hbase.dao.HBaseDao;
import com.liyaqat.hbase.dao.exception.HBaseCrudException;
import com.liyaqat.hbase.domain.Employee;
import com.liyaqat.hbase.service.exception.HbaseCrudServiceException;

@Component
public class HBaseCrudServiceImpl implements HBaseCrudService {

	@Autowired
	private HBaseDao hbasedao;
	


	public HBaseCrudServiceImpl() {
	}

	
	public void addEmployee(Employee employee)
	{
		if(employee==null)
		{
			throw new HbaseCrudServiceException("The Employee Object Can Not Be Null");
		}
		
		if(employee.getEmplid()==null || employee.getEmplid().isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id Can Not Be Null or Empty");
		}
		
		List<Employee> ids= hbasedao.getEmployeeById(employee.getEmplid());
		
		if(ids != null && !ids.isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id "+employee.getEmplid() + " Already Exists.");
		}
		try
		{	
			hbasedao.addEmployee(employee);
		}
		catch(HBaseCrudException cre)
		{
			throw new HbaseCrudServiceException(cre);
		}
	}

	public void updateEmployee(Employee employee)
	{
		if(employee==null)
		{
			throw new HbaseCrudServiceException("The Employee Object Can Not Be Null");
		}
		
		if(employee.getEmplid()==null || employee.getEmplid().isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id Can Not Be Null or Empty");
		}
		
		List<Employee> ids= hbasedao.getEmployeeById(employee.getEmplid());
		
		if(ids==null || ids.isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id "+employee.getEmplid() + " Not Found");
		}

		try
		{	
			hbasedao.updateEmployee(employee);
		}
		catch(HBaseCrudException cre)
		{
			throw new HbaseCrudServiceException(cre);
		}
	}

	public void deleteEmployee(String emplid)
	{
		if(emplid==null || emplid.isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id Can Not Be Null or Empty");
		}
		List<Employee> ids= hbasedao.getEmployeeById(emplid);
		
		if(ids==null || ids.isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id "+emplid + " Not Found");
		}
		try
		{
			hbasedao.deleteEmployee(emplid);	
		}
		catch(HBaseCrudException cre)
		{
			throw new HbaseCrudServiceException(cre);
		}
	}

	public List<Employee> getEmployeeById(String emplid)
	{
		if(emplid==null || emplid.isEmpty())
		{
			throw new HbaseCrudServiceException("The Employee Id Can Not Be Null or Empty");
		}
		List<Employee> ids= hbasedao.getEmployeeById(emplid);
		return ids;
	}


	@Override
	public List<Employee> getByObjectName(String objectName) {
		if(objectName==null || objectName.isEmpty())
		{
			throw new HbaseCrudServiceException("The objectName Can Not Be Null or Empty");
		}
		return hbasedao.getByObjectName(objectName);
	}


	@Override
	public void addBatch(List<Employee> batch) {
		
		if(batch==null || batch.isEmpty())
		{
			throw new HbaseCrudServiceException("The Batch Process List can Not Be Null or Empty");
		}
		hbasedao.addBatch(batch);
		
	}

}
