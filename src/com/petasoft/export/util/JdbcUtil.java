/*
 * Copyright 2015 Petasoft Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.petasoft.export.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.petasoft.export.conf.Configuration;

/**
 * @author ŒÈœ  2015-05-15
 */
public class JdbcUtil {
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();

	private JdbcUtil() {
	}

	public static Connection getConnection(Configuration conf) throws Exception {
		Connection conn = (Connection) threadLocal.get();
		if (conn == null) {
			String url = null;
			if (DataType.Teradata.equals(conf.get("export.db.type"))) {
				Class.forName("com.teradata.jdbc.TeraDriver");
				url = String
						.format("jdbc:teradata://%s/CLIENT_CHARSET=UTF8,TMODE=TERA,CHARSET=UTF8",
								new Object[] { conf.get("export.db.host") });
			} else if (DataType.Oracle.equals(conf.get("export.db.type"))) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
				url = String.format("jdbc:oracle:thin:@%s:%s:%s", new Object[] {
						conf.get("export.db.host"), conf.get("export.db.port"),
						conf.get("export.db.name") });
			} else if (DataType.MySQL.equals(conf.get("export.db.type"))) {
				Class.forName("com.mysql.jdbc.Driver");
				url = String
						.format("jdbc:mysql://%s:%s/%s?useUnicode=true&amp;characterEncoding=%s",
								new Object[] { conf.get("export.db.host"),
										conf.get("export.db.port"),
										conf.get("export.db.name"),
										conf.get("export.db.charset") });
			} else if (DataType.Informix.equals(conf.get("export.db.type"))) {
				Class.forName("com.informix.jdbc.IfxDriver");
				url = String
						.format("jdbc:informix-sqli://%s:%s/%s:informixserver=%s;NEWLOACLE=en_us,zh_cn,zh_tw;NEWCODESET=GBK,8859-1,819,Big5",
								new Object[] { conf.get("export.db.host"),
										conf.get("export.db.port"),
										conf.get("export.db.name"),
										conf.get("export.db.server") });
			} else if (DataType.DB2.equals(conf.get("export.db.type"))) {
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				url = String.format(
						"jdbc:db2://%s:%s/%s",
						new Object[] { conf.get("export.db.host"),
								conf.get("export.db.port"),
								conf.get("export.db.name") });
			}
			conn = DriverManager.getConnection(url, conf.get("export.db.user"),
					conf.get("export.db.pass"));
			threadLocal.set(conn);
		}
		return conn;
	}

	public static void close(ResultSet resultSet, Statement statement,
			Connection connection) {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			try {
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException ex) {
				try {
					if (connection != null) {
						connection.close();
					}
				} catch (SQLException exc) {
				}
			}
		}
	}

	public static void printResult(ResultSetMetaData metaData,
			ResultSet resultSet) throws SQLException {
		int ColumnCount = metaData.getColumnCount();

		int[] lengths = new int[ColumnCount + 1];
		for (int i = 1; i <= ColumnCount; i++) {
			lengths[i] = getMaxInt(metaData.getColumnDisplaySize(i), metaData
					.getColumnName(i).length());
		}

		for (int i = 1; i <= ColumnCount; i++) {
			System.out.print(getRpadString(metaData.getColumnName(i), " ",
					lengths[i]) + " ");
		}
		System.out.println();
		for (int i = 1; i <= ColumnCount; i++) {
			System.out.print(getRpadString("-", "-", lengths[i]) + " ");
		}
		System.out.println();
		while (resultSet.next()) {
			for (int i = 1; i <= ColumnCount; i++) {
				System.out.print(getRpadString(resultSet.getString(i), " ",
						lengths[i]) + " ");
			}
			System.out.println();
		}
	}

	private static int getMaxInt(int a, int b) {
		return a > b ? a : b;
	}

	private static String getRpadString(String data, String pad, int length) {
		if (data == null) {
			return null;
		}
		if (data.length() >= length) {
			return data;
		} else {
			StringBuffer buffer = new StringBuffer(data);
			while (getStringLength(buffer.toString()) < length) {
				buffer.append(pad);
			}
			return buffer.toString();
		}
	}

	private static int getStringLength(String s) {
		int length = 0;
		for (int i = 0; i < s.length(); i++) {
			int ascii = Character.codePointAt(s, i);
			if (ascii >= 0 && ascii <= 255)
				length++;
			else
				length += 2;
		}
		return length;
	}
}
