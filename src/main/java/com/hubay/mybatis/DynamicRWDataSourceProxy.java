package com.hubay.mybatis;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import com.hubay.mybatis.help.DataSourceHolder;

/**读写分离datasource
 * @author shuye
 * @time 2013年10月24日
 */
public class DynamicRWDataSourceProxy extends LazyConnectionDataSourceProxy {

	/**
	 * 写操作的数据源
	 */
	private DataSource writeDataSource;

	/**
	 *读操作的数据源
	 */
	private DataSource readDataSource;

	@Override
	public DataSource getTargetDataSource() {
		if (StringUtils.equals(DataSourceHolder.DataSourceType.READ.getDbName(), DataSourceHolder.getCurrentDB())) {
			setTargetDataSource(readDataSource);
		} else {
			setTargetDataSource(writeDataSource);
		}
		//将当前的DB标识删除,为了在RWPlugin插件时作判断。当用户手动调用DataSourceHolder.use*();
		//方法指定用读库还是写库时，RWPlugin拦截，StringUtils.isBlank(DataSourceHolder.getCurrentDB())判断为假，就不会改变数据库，而是用用户指定的数据库。
		//比如在插入方法执行插入数据之前调用DataSourceHolder.userRead();方法，那么RWPlugin插件不会改变数据完，此次操作就是用read数据源，而不是默认的write数据库。
		DataSourceHolder.removeCurrentDB();
		return super.getTargetDataSource();
	}

	public DataSource getWriteDataSource() {
		return writeDataSource;
	}

	public void setWriteDataSource(DataSource writeDataSource) {
		this.writeDataSource = writeDataSource;
	}

	public DataSource getReadDataSource() {
		return readDataSource;
	}

	public void setReadDataSource(DataSource readDataSource) {
		this.readDataSource = readDataSource;
	}

}
