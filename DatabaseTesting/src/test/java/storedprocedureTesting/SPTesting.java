package storedprocedureTesting;

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

import javassist.bytecode.analysis.Type;


/*Syntax                              Stored Procedure
 * {call procedure_name()}         Accept no parameters and return no value
 * {call procedure_name(?,?)}      Accept Two parameters and return no value
 * {?= call procedure_name()}      Accept no parameter and return value
 * {?= call procedure_name(?)}     Accept one parameter and return value
 */

public class SPTesting {
 Connection con=null;
 Statement stmt=null;
 ResultSet rs;
 CallableStatement cStmt;
 ResultSet rs1;
 ResultSet rs2;
 @BeforeClass
 void setup() throws SQLException {
	 con=DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root", "root");
 }
 
 @AfterClass
 void tearDown() throws SQLException {
	 con.close();
 }
 
 @Test(priority=1)
 void test_storedProcedureExists() throws SQLException {
	 stmt=con.createStatement();
	 rs=stmt.executeQuery("SHOW PROCEDURE STATUS WHERE Name='SelectAllCustomers'");  //This ExecutQuery is called through statement
	 rs.next();
	 Assert.assertEquals(rs.getString("Name"), "SelectAllCustomers");
 }
 
 @Test(priority=2)
 void test_SelectAllCustomers() throws SQLException {
	 // ResultSet from Stored Procedure
	 cStmt=con.prepareCall("{CALL SelectAllCustomers()}"); //When you need to call stored procedure with query than you have to 
	                                                       //prepare first the executable call statement
	 rs1=cStmt.executeQuery();   // This is ResultSet1     //This executeQuery is execute through CalleableStatement
	 // ResultSet from Query
	 Statement stmt=con.createStatement();
	 rs2=stmt.executeQuery("select * from customers"); //ResultSet2
	 
	 Assert.assertEquals(compareResultSets(rs1,rs2), true);
 } 
 
 @Test(priority=3)
 void test_SelectAllCustomersByCity() throws SQLException {
	 // ResultSet from Stored Procedure
	 cStmt=con.prepareCall("{CALL SelectAllCustomersByCity(?)}"); //When you need to call stored procedure with query than you have to 
	                                                       //prepare first the executable call statement
	 cStmt.setString(1, "Singapore");
	 rs1=cStmt.executeQuery();   // This is ResultSet1     //This executeQuery is execute through CalleableStatement
	 // ResultSet from Query
	 Statement stmt=con.createStatement();
	 rs2=stmt.executeQuery("SELECT * FROM  customers WHERE city='Singapore'"); //ResultSet2
	 
	 Assert.assertEquals(compareResultSets(rs1,rs2), true);
 } 
 
 @Test(priority=4)
 void test_SelectAllCustomersByCityAndPinCode() throws SQLException {
	 // ResultSet from Stored Procedure
	 cStmt=con.prepareCall("{CALL SelectAllCustomersByCityAndPin(?,?)}"); //When you need to call stored procedure with query than you have to 
	                                                       //prepare first the executable call statement
	 cStmt.setString(1, "Singapore");
	 cStmt.setString(2, "079903");
	 rs1=cStmt.executeQuery();   // This is ResultSet1     //This executeQuery is execute through CalleableStatement
	 // ResultSet from Query
	 Statement stmt=con.createStatement();
	 rs2=stmt.executeQuery("ELECT * FROM  customers WHERE city='Singapore' AND postalCode= '079903'"); //ResultSet2
	 
	 Assert.assertEquals(compareResultSets(rs1,rs2), true);
 } 
 
 @Test(priority=5)
 void test_get_order_by_cust() throws SQLException {
	 cStmt=con.prepareCall("{call get_order_by_cust(?,?,?,?,?)}");
	 cStmt.setInt(1, 141);
	 
	 cStmt.registerOutParameter(2, Types.INTEGER);
	 cStmt.registerOutParameter(3, Types.INTEGER);
	 cStmt.registerOutParameter(3, Types.INTEGER);
	 cStmt.registerOutParameter(5, Types.INTEGER);
	 
	 cStmt.executeQuery();
	 int shipped=cStmt.getInt(2);
	 int canceled=cStmt.getInt(3);
	 int resolved=cStmt.getInt(4);
	 int disputed=cStmt.getInt(5);
	 
	 //System.out.println(shipped+""+canceled+""+resolved+""+disputed);
	 
	 Statement stmt=con.createStatement();
	 rs=stmt.executeQuery("SELECT(SELECT COUNT(*) AS 'shipped' FROM Orders WHERE customerNumber=141 AND status = 'Shipped') AS Shipped; (SELECT COUNT(*) AS 'canceled' FROM Orders WHERE customerNumber=141 AND status = 'Canceled') AS Canceled; (SELECT COUNT(*) AS 'resolved' FROM Orders WHERE customerNumber=141 AND status = 'Resolved') AS Resolved; (SELECT COUNT(*) AS 'disputed' FROM Orders WHERE customerNumber=141 AND status = 'Disputed') AS Disputed;");
	 rs.next();
	 
	 int exp_shipped=rs.getInt("shipped");
	 int exp_canceled=rs.getInt("canceled");
	 int exp_resolved=rs.getInt("resolved");
	 int exp_disputed=rs.getInt("disputed");
	 
	 if(shipped==exp_shipped && canceled==exp_canceled && resolved==exp_resolved && disputed==exp_disputed);
	 Assert.assertTrue(true);
	 else
		 Assert.assertTrue(false);
	 
 }
 
 @Test(priority=6)
 void test_GetCustomerShipping() throws SQLException {
	 cStmt=con.prepareCall("{call GetCustomerShipping(?,?)}");
	 cStmt.setInt(1, 112);
	 
	 cStmt.registerOutParameter(2, Types.VARCHAR);

	 
	 cStmt.executeQuery();
	 String shippingTime=cStmt.getString(2);
	
	 
	 //System.out.println(shipped+""+canceled+""+resolved+""+disputed);
	 
	 Statement stmt=con.createStatement();
	 rs=stmt.executeQuery("SELECT country CASE WHEN country='USA' THEN '2-day Shipping' WHEN country='Canada' THEN '3-day Shipping' ELSE '5-day Shipping' END AS ShippingTime FROM customer WHERE customerNumber=112");
	 rs.next();
	 
	 String exp_shippingTime= rs.getString("ShippingTime");
	 Assert.assertEquals(shippingTime, exp_shippingTime);
	 
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
