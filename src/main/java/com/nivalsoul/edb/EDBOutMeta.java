package com.nivalsoul.edb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

public class EDBOutMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = EDBOutMeta.class;
	private String url;
	private String collection;

	private String jobId;
	private String orderByKey;
	private String driverName;
	private String mysqlUrl;
	private String username;
	private String password;
	private String table;

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	
	public void setOrderByKey(String orderByKey) {
		this.orderByKey = orderByKey;
	}

	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}

	public void setMysqlUrl(String mysqlUrl) {
		this.mysqlUrl = mysqlUrl;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setDefault() {
		this.url = "10.65.6.143:8300";
		this.collection = "clusterName.index.table";
	}

	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getCollection() {
		return this.collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public String getXML() throws KettleValueException {
		StringBuilder retval = new StringBuilder();
		retval.append("    ").append(XMLHandler.addTagValue("url", this.url));
		retval.append("    ").append(XMLHandler.addTagValue("collection", this.collection));
		return retval.toString();
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleXMLException {
		setUrl(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "url")));
		setCollection(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "collection")));
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		rep.saveStepAttribute(id_transformation, id_step, "url", this.url);
		rep.saveStepAttribute(id_transformation, id_step, "collection", this.collection);
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		this.url = rep.getStepAttributeString(id_step, "url");
		this.collection = rep.getStepAttributeString(id_step, "collection");
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
			String[] input, String[] output, RowMetaInterface info) {
		CheckResult cr = new CheckResult(1,
				BaseMessages.getString(PKG, "ExcelInputMeta.CheckResult.AcceptFilenamesOk", new String[0]), stepMeta);
		remarks.add(cr);
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		return new EDBOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new EDBOutData();
	}

	public boolean loadDatas(List<Map<String, Object>> olist) {
		if (olist.size() == 0) {
			return true;
		}

		String result = doPostESql(olist);
		// 再次尝试
		if (result.equals("OtherError")) {
			logBasic("post data to EDB second time...");
			result = doPostESql(olist);
			// 再次尝试
			if (result.equals("OtherError")) {
				logBasic("post data to EDB third time...");
				result = doPostESql(olist);
			}
		}

		if (result.equals("OK")) {
			// 最后一条记录的排序字段(可能是"DEP_ACTION_TIME")值最大
			try {
				Map<String, Object> map = olist.get(olist.size() - 1);
				String orderByValue = String.valueOf(map.get(orderByKey));
				orderByValue = orderByValue.replaceAll("T", " ").replaceAll("00Z", "");
				logBasic("jobId="+jobId+"#orderByKey="+orderByKey+"#orderByValue="+orderByValue);
				updateJobInfo(jobId, orderByValue);
			} catch (Exception e) {
				logBasic("update jobinfo error..."+e.getMessage());
				e.printStackTrace();
			}
		}

		return result.equals("OK");
	}

	/**
	 * 更新job信息
	 * 
	 * @param jobId
	 * @param maxDepActionTime
	 *            最大的dep_action_time
	 * @return
	 */
	private boolean updateJobInfo(String jobId, String orderByValue) {
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(mysqlUrl, username, password);
			Statement stmt = con.createStatement();
			String sql = "update " + table + " set orderByValue='" + orderByValue + "' where jobId='" + jobId + "'";
			logBasic("update sql: " + sql);
			stmt.execute(sql);
			stmt.close();
			con.close();
			return true;
		} catch (Exception e) {
			logBasic("ChinacloudException:UpdateJobInfoFailed");
			return false;
		}
	}

	public String doPostESql(List<Map<String, Object>> olist) {
		String[] info = collection.split("[.]");
		String clusterName = info[0]; // 集群名称
		String database = info[1]; // 索引名
		String table = info[2]; // 表名
		String ip = url.split("[:]")[0];
		int port = Integer.parseInt(url.split("[:]")[1]);
		SQL4ESUtil sql4esUtil = new SQL4ESUtil(clusterName, ip, port);
		String result = sql4esUtil.bulkRequest(database, table, olist);
		if (!"OK".equals(result)) {
			logBasic("ChinacloudException:" + result);
		}
		return result;
	}

}
