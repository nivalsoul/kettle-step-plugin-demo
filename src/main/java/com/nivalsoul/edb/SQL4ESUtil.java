package com.nivalsoul.edb;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.alibaba.fastjson.JSON;

public class SQL4ESUtil {
	
	private String clasterName = "elasticsearch";
	private String ip = "192.168.1.104";
	private int port = 9400;
	
	public SQL4ESUtil(String clasterName, String ip, int port) {
		if(clasterName!=null && !clasterName.equals(""))
		    this.clasterName = clasterName;
		this.ip = ip;
		this.port = port;
	}
	
	
	private Client getClient() throws UnknownHostException {
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", clasterName).build();
		Client client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
		return client;
	}
	
	/**
	 * 单次提交
	 * @param database 索引库名称
	 * @param table    表名
	 * @param data     一行数据，map类型，指定主键的话key为"_id"
	 * @return 
	 */
	public boolean singleRequest(String database, String table, Map<String, Object> data) {
		boolean status = false;
		try {
			Client client = getClient();
			IndexResponse response;
			if(data.containsKey("_id")){
				String id = String.valueOf(data.get("_id"));
				data.remove("_id");
				response = client.prepareIndex(database, table, id)
						.setSource(JSON.toJSONString(data)).get();
			}else {
				response = client.prepareIndex(database, table)
						.setSource(JSON.toJSONString(data)).get();
			}
			status = response.isCreated();
			client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return status;
	}

	/**
	 * bulk批量提交
	 * @param database 索引库名称
	 * @param table    表名
	 * @param rows     多行数据，每行都是一个map类型，指定主键的话key为"_id"
	 * @return         成功返回OK，否则返回错误信息
	 */
	public String bulkRequest(String database, String table, List<Map<String, Object>> rows) {
		String result = "OK";
		try {
			Client client = getClient();
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Map<String, Object> data : rows) {
				if(data.containsKey("_id")){
					String id = String.valueOf(data.get("_id"));
					data.remove("_id");
					bulkRequest.add(client.prepareIndex(database, table, id)
						.setSource(JSON.toJSONString(data)));
				}else{
					bulkRequest.add(client.prepareIndex(database, table)
							.setSource(JSON.toJSONString(data)));
				}
			}
			BulkResponse bulkResponse = bulkRequest.get();
			if (bulkResponse.hasFailures()) {
			     result = bulkResponse.buildFailureMessage();
			}
			client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return result;
	}
}
