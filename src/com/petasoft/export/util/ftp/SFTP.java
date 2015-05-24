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
package com.petasoft.export.util.ftp;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

/**
 * @author 伍鲜 2015-05-15
 */
public class SFTP {
	/**
	 * SFTP传送文件
	 * 
	 * @param host
	 *            SFTP主机
	 * @param port
	 *            SFTP端口
	 * @param user
	 *            SFTP用户
	 * @param pass
	 *            SFTP密码
	 * @param path
	 *            SFTP路径
	 * @param file
	 *            SFTP文件
	 * @param cset
	 *            SFTP编码
	 * @param time
	 *            SFTP超时
	 * @return
	 * @throws Exception
	 */
	public static boolean ftpFile(String host, String port, String user,
			String pass, String path, String file, String cset, String time)
			throws Exception {
		File srcFile = new File(file);
		Session session = null;
		Channel channel = null;
		ChannelSftp sftp = null;

		// 创建JSch对象
		JSch jsch = new JSch();

		// 根据用户名，主机地址，端口获取一个Session对象
		session = jsch.getSession(user, host, Integer.parseInt(port));
		if (pass != null) {
			// 设置密码
			session.setPassword(pass);
		}
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		// 为Session对象设置properties
		session.setConfig(config);
		// 设置timeout时间
		session.setTimeout(Integer.valueOf(time) * 1000);
		// 通过Session建立链接
		session.connect();
		// 打开SFTP通道
		channel = session.openChannel("sftp");
		// 建立SFTP通道的连接
		channel.connect();
		sftp = (ChannelSftp) channel;

		sftp.cd(path);
		sftp.put(new FileInputStream(file), srcFile.getName(),
				ChannelSftp.OVERWRITE);

		sftp.quit();
		sftp.disconnect();
		session.disconnect();
		return true;
	}
}
