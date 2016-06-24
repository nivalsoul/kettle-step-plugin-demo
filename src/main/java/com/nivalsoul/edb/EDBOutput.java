package com.nivalsoul.edb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


public class EDBOutput extends BaseStep implements StepInterface {
    private EDBOutData data;
    private EDBOutMeta meta;
    private static AtomicInteger count = new AtomicInteger(0);
    private static List<Map<String,Object>> olist = new ArrayList<Map<String, Object>>();
    private static String jobId ;
    private static String orderByKey ;
    private static String maxDepActionTime ;
    private static int rowCount=0;
    

    public EDBOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((EDBOutMeta) smi);
        this.data = ((EDBOutData) sdi);
        Object[] row = getRow();
        RowMetaInterface rowMeta = getInputRowMeta();
        
        if(rowMeta==null){
            setOutputDone();
            olist.clear();
            return false;
        }
        String[] fields = rowMeta.getFieldNames();   
        
        if (row == null) {//没有数据需要处理了
            boolean result = meta.loadDatas(olist); 
            //失败则停止该步骤
            if (!result) {
            	logBasic("post data to EDB failed, stop the step!");
            	stopAll();
            }
            setOutputDone();
            olist.clear();

            return false;
        }  

        //统计行数
        rowCount++;

		if(count.intValue() == KettleConstants.COMMIT_BATCH){
			boolean result = meta.loadDatas(olist); 
            olist.clear();
            count.set(0);
            //失败则停止该步骤
            if (!result) {
            	logBasic("post data to EDB failed, stop the step!");
				stopAll();
			}
        }
		
        if (this.first) {
            first = false;
            this.data.outputRowMeta = getInputRowMeta().clone();
            this.meta.getFields(this.data.outputRowMeta, getStepname(), null, null, this);
            jobId =  getVariable("jobId");
            orderByKey =  getVariable("orderByKey");
        }

        Map omap = new HashMap();  
        boolean flag = true;
        for(int i = 0; i < row.length; i++){
            if(row[i] != null){
                Object value=row[i];
                omap.put(fields[i],value);
            }
        }  
        olist.add(omap);        
        count.incrementAndGet();
        putRow(this.data.outputRowMeta, row);
        return true;
    }
   
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((EDBOutMeta) smi);
        this.data = ((EDBOutData) sdi);
        //设置配置项
    	this.meta.setJobId(getVariable("jobId"));
    	this.meta.setOrderByKey(getVariable("orderByKey"));
    	this.meta.setDriverName(getVariable("driver"));
    	this.meta.setMysqlUrl(getVariable("url"));
    	this.meta.setUsername(getVariable("username"));
    	this.meta.setPassword(getVariable("password"));
    	this.meta.setTable(getVariable("table"));
    	
        return super.init(smi, sdi);
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((EDBOutMeta) smi);
        this.data = ((EDBOutData) sdi);
        super.dispose(smi, sdi);
    }

}