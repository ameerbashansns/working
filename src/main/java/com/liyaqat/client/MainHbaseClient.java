package com.liyaqat.client;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.liyaqat.hbase.domain.Employee;
import com.liyaqat.hbase.service.HBaseCrudService;

public class MainHbaseClient {

	public MainHbaseClient()
	{
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		String operation="delete";
		ClassPathXmlApplicationContext context= new ClassPathXmlApplicationContext("hbasespring-context.xml");
		try
		{
			System.out.println(context);
			HBaseCrudService service=context.getBean("HBaseCrudServiceImpl", HBaseCrudService.class);
			if(operation.equalsIgnoreCase("batch"))
			{
				List<Employee> employees= new ArrayList<Employee>();
				for(int i=400000;i<700000;i++)
				{
					employees.add(buildEmployee("batch", i));
				}
				service.addBatch(employees);
			}
			else
			{
				
				Employee emp= new Employee();
				emp.setEmplid("abc126");
				emp.setFirst("abcf");
				emp.setLast("abcl");
				emp.setMiddle("abcm");
				emp.setDisplayname("abcf abcm abcl");
				if(operation.equalsIgnoreCase("add"))
				{
					service.addEmployee(emp);	
				}
				else if(operation.equalsIgnoreCase("update"))
				{
					service.updateEmployee(emp);
				}
				else if(operation.equalsIgnoreCase("delete"))
				{
					service.deleteEmployee(emp.getEmplid());
				}
				else if(operation.equalsIgnoreCase("select"))
				{
					System.out.println(service.getEmployeeById("batch10"));
				}
				else if(operation.equalsIgnoreCase("scan"))
				{
					long start= System.currentTimeMillis();
					System.out.println(service.getByObjectName("EmployeeData").size());
					long end= System.currentTimeMillis();
					System.out.println((end-start)/1000);
				}
				
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			context.close();
		}
	}
	
	private static Employee buildEmployee(String prefix,int id)
	{
		Employee emp= new Employee();
		//
		emp.setEmplid(prefix+id);
		emp.setFirst(prefix+id);
		emp.setLast(prefix+id);
		emp.setMiddle(prefix+id);
		emp.setDisplayname(emp.getFirst() + " "+emp.getMiddle() +" "+emp.getLast());
		return emp;
	}
}
