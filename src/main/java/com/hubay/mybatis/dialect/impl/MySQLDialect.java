package com.hubay.mybatis.dialect.impl;

import com.hubay.mybatis.dialect.Dialect;

/**
 * 
 * @author shuye
 * @time 上午11:57:37
 */
public class MySQLDialect extends Dialect {

	
	@Override
	public String buildPageSql(String sql, int offset, int pageSize) {
		
		StringBuilder builder = new StringBuilder(sql);
		return builder.append(" limit ").append(offset).append(",").append(pageSize).toString();
	}

}
