package com.hubay.mybatis;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import com.hubay.mybatis.help.DataSourceHolder;

/**多个读库和写库的读写分离datasource
 * @author shuye
 * @time 2013年10月24日
 */
public class DynamicMultiRWDataSourceProxy extends LazyConnectionDataSourceProxy {
	
	
	private DataSourceBalanceRoute route = new DynamicMultiRWDataSourceProxy.DataSourceBalanceRoute();
	
	/**
	 * 写操作的数据源
	 */
	private List<DataSource> writeDataSources;

	/**
	 * 读操作的数据源
	 */
	private List<DataSource> readDataSources;

	@Override
	public DataSource getTargetDataSource() {
		if (StringUtils.equals(DataSourceHolder.DataSourceType.READ.getDbName(), DataSourceHolder.getCurrentDB())) {
			setTargetDataSource(readDataSources.get(route.balanceRead()));
		} else {
			setTargetDataSource(writeDataSources.get(route.balanceWrite()));
		}
		//将当前的DB标识删除,为了在RWPlugin插件时作判断。当用户手动调用DataSourceHolder.use*();
		//方法指定用读库还是写库时，RWPlugin拦截，StringUtils.isBlank(DataSourceHolder.getCurrentDB())判断为假，就不会改变数据库，而是用用户指定的数据库。
		//比如在插入方法执行插入数据之前调用DataSourceHolder.userRead();方法，那么RWPlugin插件不会改变数据完，此次操作就是用read数据源，而不是默认的write数据库。
		DataSourceHolder.removeCurrentDB();
		return super.getTargetDataSource();
	}
	
	
	public List<DataSource> getWriteDataSources() {
		return writeDataSources;
	}

	public void setWriteDataSources(List<DataSource> writeDataSources) {
		this.writeDataSources = writeDataSources;
	}

	public List<DataSource> getReadDataSources() {
		return readDataSources;
	}

	public void setReadDataSources(List<DataSource> readDataSources) {
		this.readDataSources = readDataSources;
	}

	/**用于路由多个读和写库，选择其中一个来读或者写
	 * @author shuye
	 * @time 2013年10月25日
	 */
     class DataSourceBalanceRoute{
		
    	 
    	 private AtomicInteger count = new AtomicInteger(0) ;
    	 
    	 
    	 public int balanceRead(){
    		 int index = Math.abs(count.incrementAndGet())% readDataSources.size();
    		return  index;
    	 }
    	 
    	 public int balanceWrite(){
    	     int index = Math.abs(count.incrementAndGet())% writeDataSources.size();
    		 return  index;
    	 }
		
	}

}
