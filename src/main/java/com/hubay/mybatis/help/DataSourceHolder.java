package com.hubay.mybatis.help;

/**持有数据源类型
 * @author shuye
 * @time 2013-7-24
 */
public final class DataSourceHolder {

	private DataSourceHolder() {
	}

	private static final ThreadLocal<String> holder = new ThreadLocal<String>();

	/**
	 * 
	 * @param dbName
	 */
	public static void setCurrentDB(DataSourceType databaseType) {
		holder.set(databaseType.getDbName());
	}

	/**
	 * 
	 * @return
	 */
	public static String getCurrentDB() {
		return holder.get();
	}

	/**
	 * 
	 */
	public static void removeCurrentDB() {
		holder.remove();
	}
	
	/**
	 * 
	 */
	public static void useRead(){
		removeCurrentDB();
		setCurrentDB(DataSourceType.READ);
	}
	
	/**
	 * 
	 */
	public static void useWrite(){
		removeCurrentDB();
		setCurrentDB(DataSourceType.WRITE);
	}

	public enum DataSourceType {
		READ("read"), WRITE("write");
		private String dbName;

		private DataSourceType(String _dbName) {
			this.dbName = _dbName;
		}

		public String getDbName() {
			return dbName;
		}
	}
}
