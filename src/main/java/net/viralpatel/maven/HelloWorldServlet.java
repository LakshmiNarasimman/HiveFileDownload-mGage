package net.viralpatel.maven;

import java.io.File;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;


public class HelloWorldServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1031422249396784970L;
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		resp.setContentType("text/html");
		
		try {
			
			PrintWriter out = resp.getWriter();
			Class.forName(driverName);
			Date d1=new Date();
			SimpleDateFormat sdf22 = new SimpleDateFormat("HH:mm:ss");
	      	System.out.println( sdf22.format(d1));
			Connection con = DriverManager.getConnection("jdbc:hive2://10.130.121.181:10000/poc", "hduser", "noah");
		    out.print("Conected to Hive...!");
			Statement stmt = con.createStatement();
		   // String tableName = "test";
		    //stmt.execute("drop table if exists " + tableName);
		   // stmt.execute("create table " + tableName + " (key int, value string)");
			//String sql="select sdate,esmeaddr,cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr=70836900000000";
            String esmeAddr ="70836900000000";
            String reportStartDate="2017-06-06";
            String reportEndDate="2017-06-06";
            String limit="1000";
		    String sql = "select cust_send,dest,msg,stime,dtime,dn_status,mid,rp,operator,circle,cust_mid,first_attempt,second_attempt,third_attempt,fourth_attempt,fifth_attempt,term_operator,term_circle from poc.sms_log where esmeaddr='" + esmeAddr + "' and sdate between '" + reportStartDate + "' and '" + reportEndDate + "' limit " + limit;
		   
		    ResultSet res = stmt.executeQuery(sql);
		    CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");

			// file name
			final String FILE_NAME = "/home/bigdata/bigdata_noahdata/JDBC-HIVE-FILE/sms_log_with_where.csv";
			
			// creating the file object
			File file = new File(FILE_NAME);
			
			// creating file writer object
			FileWriter fw = new FileWriter(file);

			// creating the csv printer object
			CSVPrinter printer = new CSVPrinter(fw, format);

			// reading the query from user as input
			
			
			// printing the result in 'CSV' file
			printer.printRecords(res);
			
			System.out.println("Query has been executed successfully...");
			
			// closing all resources
			Date d2=new Date();
	      	System.out.println( sdf22.format(d2));
		    
			out.print("Hello World from Servlet");
			out.flush();
			out.close();
			printer.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
