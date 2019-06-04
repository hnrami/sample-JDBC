package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class SampleJDBCCall {

	
	
	 static BufferedWriter writer ;
	   
	static CountDownLatch latch;
	static ExecutorService executor;
	static SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

//	 static String inputFile= "/input.txt";
//	 static String outputFile="/result.txt";
	public static void main(String[] args) {
		try {
			System.out.println("1.inputFile 2.outputfile 3.numberofthread 4.url 5.username 6.password");
			String inputFile = args[0];
			String outputFile = args[1];
			int numberthread = Integer.parseInt(args[2]);
			String drivername=args[3];
			String url=args[4];
			String usrname=args[5];
			String password=args[6];
			List<String> resultCurl = processInputFile(inputFile);
			System.out.println("size of SQL statements is " + resultCurl.size());
			
			//Class.forName("org.postgresql.Driver");
			Class.forName(drivername);
//			Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5455/postgres", "postgres",
//					"");
			Connection connection = DriverManager.getConnection(url, usrname,
					password);
			System.out.println("Opened database successfully");

			

			Scanner myObj = new Scanner(System.in);
			System.out.println("are you sure you want start Thread? please enter your answer yes/no/y/n");

			String result = myObj.nextLine();
			System.out.println("your Answer is : " + result);
			if (result.equalsIgnoreCase("yes") || result.equalsIgnoreCase("y")) {
				int thread = 0;
				// Enter Read File where we read CURL STATMENT

//			
				writer= new BufferedWriter(new FileWriter(outputFile));
				executor = Executors.newFixedThreadPool(numberthread);
				latch = new CountDownLatch(resultCurl.size());
				System.out.println("we start Thread time is ::" + formatter.format(new Date()));
				for (String query : resultCurl) {
					executor.submit(() -> {
						Statement statement;
						try {
							statement = connection.createStatement();
							dbCall( statement,query);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						//callCurl(curlURL);
					});

				}
			} else {
				System.out.println("we exist for program");
				writer.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				latch.await();
				writer.close();

				System.out.println("complate CSV Write Time is " + formatter.format(new Date()));
				executor.shutdownNow();

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	
	private static List<String> processInputFile(String inputFilePath) throws IOException {
		List<String> inputList = new ArrayList<String>();

		File inputF = new File(inputFilePath);
		InputStream inputFS = new FileInputStream(inputF);
		BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
		inputList = br.lines().collect(Collectors.toList());
		br.close();

		return inputList;
	}

	public static void dbCall(Statement statement,String query) {

		try {
			ResultSet  resultSet = statement
			           .executeQuery(query);
			  while(resultSet.next()) {
			   System.out.println("result:::"+resultSet.getString(2));
			
			   writer.append(resultSet.getString(2));
			   writer.append("\n");
			  }
			  latch.countDown();
			  statement.close();
			
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
	}
}
