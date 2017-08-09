package com.passparam.download;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class DownloadHDFS
 */
public class DownloadHDFS extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public DownloadHDFS() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#service(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void service(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		String esmeAddress = request.getParameter("esmeaddress");
		String fromDate = request.getParameter("fromdate");
		String toDate = request.getParameter("todate");
		String limit = request.getParameter("limit");

		long longLimit = 1000;
		try {
			longLimit = Long.parseLong(limit);
		} catch (Exception e) {
		}

		String error = validateInputs(esmeAddress, fromDate, toDate);

		if ("".equals(error)) {
			String fileName = HDFSDownloader.process(esmeAddress, fromDate,
					toDate, longLimit);
			System.out.println("File Name : " + fileName);
			if (fileName != null) {
				try {
					File downloadFile = new File(fileName);
					if (downloadFile.exists()) {
						response.setHeader("Cache-Control", "must-revalidate");
						response.setContentType("application/zip");
						response.addHeader("Content-Disposition",
								"attachment; filename=" + fileName);
						InputStream input = new FileInputStream(downloadFile);
						ByteArrayOutputStream ous = new ByteArrayOutputStream();
						ServletOutputStream out = response.getOutputStream();
						int readBytes = 0;

						while ((readBytes = input.read()) != -1) {
							ous.write(readBytes);
						}

						response.setContentLength(ous.toByteArray().length);
						out.write(ous.toByteArray());
						if (input != null) {
							input.close();
						}
					} else {
						writeError(response,
								"Error while generating the CSV file.");
					}
				} catch (Exception e) {
					writeError(response, "Error while generating the CSV file."
							+ e);
				}

			} else {
				writeError(response, "Error while generating the CSV file.");
			}
		} else {
			writeError(response, error);
		}
	}

	private void writeError(HttpServletResponse response, String string)
			throws IOException {
		PrintWriter writer = response.getWriter();
		writer.write(string);
		writer.flush();
	}

	private String validateInputs(String esmeAddress, String fromDate,
			String toDate) {
		esmeAddress = nullCheck(esmeAddress);

		String error = "";
		if ("".equals(esmeAddress))
			error = " Invalid Esme Address Specified.";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setLenient(false);

		try {
			sdf.parse(fromDate);
		} catch (ParseException e) {
			error = "Invalid From date specified.";
		}

		try {
			sdf.parse(toDate);
		} catch (ParseException e) {
			error = "Invalid To date specified.";
		}

		return error;
	}

	private static String nullCheck(Object aObj) {
		return aObj == null ? "" : aObj.toString();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		service(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		service(request, response);
	}

}
