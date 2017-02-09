package com.hubay.mybatis;

import java.sql.Connection;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.slf4j.Logger;

import com.hubay.lang.helper.GodHands;
import com.hubay.lang.helper.LogFactory;
import com.hubay.mybatis.help.DataSourceHolder;

/**
 * 
 * @author shuye
 * @time 下午4:00:37
 */
@Intercepts({ @Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class }) })
public class RWPlugin implements Interceptor {

	protected final static Logger LOG = LogFactory.createLogger(RWPlugin.class);

	@Override
	public Object intercept(Invocation invocation) throws Throwable {

		//每次在DynamicRWDataSourceProxy动态设置完DataSource之后都会清空。
		//如果不是空，说明是用户手动调用了DataSourceHolder.use*();方法指定此次操作使用的数据源，那插件就不再改变数据源。
		if (StringUtils.isBlank(DataSourceHolder.getCurrentDB())) {
			StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
			MappedStatement statement = null;
			if (statementHandler instanceof RoutingStatementHandler) {
				StatementHandler delegate = (StatementHandler) GodHands.getFieldValue(statementHandler, "delegate");
				statement = (MappedStatement) GodHands.getFieldValue(delegate, "mappedStatement");
			} else {
				statement = (MappedStatement) GodHands.getFieldValue(statementHandler, "mappedStatement");
			}
			DataSourceHolder.useRead();
			int type = statement.getSqlCommandType().compareTo(SqlCommandType.SELECT);
			if (type < 0) {
				DataSourceHolder.useWrite();
				if (LOG.isDebugEnabled()) {
					LOG.debug("切换到库 " + DataSourceHolder.getCurrentDB());
				}
			}
		}
		return invocation.proceed();
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	@Override
	public void setProperties(Properties properties) {

	}

}
