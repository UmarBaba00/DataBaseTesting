package storedFunctionTesting;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.hadoop.shaded.org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;



public class sFTesting {
	
	Connection con=null;
	Statement stmt;    //used to call query
	ResultSet rs;
	ResultSet rs1;
	ResultSet rs2;
	CallableStatement cStmt; 
	@BeforeClass
	void setUp() throws SQLException {
		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}
	
	@AfterClass
	void tearDown() throws SQLException {
		con.close();
	}
	
	@Test(priority=1)
	void test_storedFunctionExixts() throws SQLException {
		rs=con.createStatement().executeQuery("SHOW FUNCTION STATUS WHERE Name='CustomerLevel'");
		rs.next();
		//rs.getString("Name");
		Assert.assertEquals(rs.getString("Name"), "CustomerLevel");
	
	}
	
	
	@Test(priority=2)
	void test_CustomerLevel_with_SQLStatement() throws SQLException {
		rs1=con.createStatement().executeQuery("SELECT customerName, CustomerLevel(creditLimit) FROM customer");
		rs2=con.createStatement().executeQuery("SELECT customerName, CASE WHEN creditLimit > 50000 THEN 'PLATINUM' WHEN creditLimit >= 10000 AND creditLimit < 50000 THEN 'GOLD' WHEN creditLimit < 10000 THEN 'SILVER' END as customerlevel FROM customers");
		rs.next();
		//rs.getString("Name");
		//Assert.assertEquals(rs.getString("Name"), "CustomerLevel");
		 Assert.assertEquals(compareResultSets(rs1,rs2), true);
	
	}
	
	
	@Test(priority=3)
	void test_CustomerLevel_with_SQLStoredProcedure() throws SQLException {
		cStmt=con.prepareCall("{CALL GetCustomerLevel(?,?)}");
		cStmt.setInt(1, 131);
		cStmt.registerOutParameter(2, Types.VARCHAR);
		
		cStmt.executeQuery();
		
		String custlevel=cStmt.getString(2);
		
		rs=con.createStatement().executeQuery("SELECT customerName, CASE WHEN creditLimit > 50000 THEN 'PLATINUM' WHEN creditLimit >= 10000 AND creditLimit < 50000 THEN 'GOLD' WHEN creditLimit < 10000 THEN 'SILVER' END as customerlevel FROM customers WHERE customerNumber = 131");
	    rs.next();
	    String exp_custlevel= rs.getString("customerlevel");
	    Assert.assertEquals(custlevel, exp_custlevel);
	}
	
	
	 // Compare Two ResultSets
	 public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
		 while(resultSet1.next()) { // get first row of first table
			 resultSet2.next(); // get first row of second table
			 int count = resultSet1.getMetaData().getColumnCount();  //compare all the column data
			 for (int i=1; i<=count; i++) {
				 if(!StringUtils.equals(resultSet1.getString(i), resultSet2.getString(i))) {
					 return false;
				 }
			 }
		 }
		 return true;
	 }

}
