package com.nivalsoul.edb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put("aa", "afsdaf");
    	map.put("bb", 123);
    	System.out.println(String.valueOf(map.get("bb")));
    	List<Map<String, Object>> list   = new ArrayList<Map<String,Object>>();
    	list.add(map);
    	//new SQL4ESUtil("", "192.168.1.104", 9400).bulkRequest("testindex", "tt", list);
    }
    
    public static void execMysql() {
    	String driverName = "com.mysql.jdbc.Driver";
    	String url = "jdbc:mysql://localhost:3306/jobinfo";
    	String username = "root";
    	String password = "root";
    	String table = "job";
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(url, username, password);
			Statement stmt = con.createStatement();
			String sql = "insert into job(jobId,rowCount) values('job3',0)";
			System.out.println(sql);
			stmt.execute(sql);
			stmt.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
