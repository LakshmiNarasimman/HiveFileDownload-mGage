package com.passparam.download;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class HDFSDownloader {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	private static String sql = "select " + "cust_send, " + "dest, " + "msg, "
			+ "stime, " + "dtime, " + "dn_status, " + "mid, " + "rp, "
			+ "operator, " + "circle, " + "cust_mid, " + "first_attempt, "
			+ "second_attempt, " + "third_attempt, " + "fourth_attempt, "
			+ "fifth_attempt, " + "term_operator, " + "term_circle "
			+ "from poc.sms_log ";

	static {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String process(String esmeAddress, String fromDate,
			String toDate, long limit) {
		String returnValue = null;
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://10.130.121.181:10000/poc", "hduser", "noah");

			String whereCondition = " where esmeaddr = '" + esmeAddress
					+ "' and sdate between '" + fromDate + "' and '" + toDate
					+ "' limit "+limit;
			
			String sql1 = sql + whereCondition;
			System.out.println("SQL is : '"+sql1+"'");
			
			PreparedStatement prepareStatement = con.prepareStatement(sql1);

//			prepareStatement.setString(1, esmeAddress);
//			prepareStatement.setString(2, fromDate);
//			prepareStatement.setString(3, toDate);

			ResultSet res = prepareStatement.executeQuery();
			CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");
          ///home/hduser/JDBC-HIVE-FILE/
			String FILE_NAME = "/home/bigdata/bigdata_noahdata/JDBC-HIVE-FILE/"
					+ System.currentTimeMillis() + ".csv";

			// creating the file object
			File file = new File(FILE_NAME);

			// creating file writer object
			FileWriter fw = new FileWriter(file);

			// creating the csv printer object
			CSVPrinter printer = new CSVPrinter(fw, format);

			// printing the result in 'CSV' file
			printer.printRecords(res);

			System.out.println("Query has been executed successfully...");
			printer.close();
			res.close();
			prepareStatement.close();
			con.close();
			String zipFileName = file.getName().concat(".zip");
			 
			System.out.println("zipFileName : " +zipFileName);
			File zipFile = new File("/home/bigdata/bigdata_noahdata/JDBC-HIVE-FILE/"+zipFileName);
			
			FileInputStream fis= new FileInputStream(FILE_NAME);
            FileOutputStream fos = new FileOutputStream(zipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);
 
            zos.putNextEntry(new ZipEntry(file.getName()));
 
            byte[] bytes = Files.readAllBytes(Paths.get(FILE_NAME));
            zos.write(bytes, 0, bytes.length);

            zos.flush();
            zos.close();
       
            
			// closing all resources
			

			returnValue = zipFile.getAbsolutePath();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return returnValue;
	}
}
