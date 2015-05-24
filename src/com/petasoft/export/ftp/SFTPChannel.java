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
package com.petasoft.export.ftp;

import java.util.Map;
import java.util.Properties;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * @author 伍鲜 2015-05-15
 */
public class SFTPChannel {
	Session session = null;
	Channel channel = null;

	public ChannelSftp getChannel(Map<String, String> sftp, int timeout)
			throws JSchException {

		String ftpHost = sftp.get(SFTPConstants.SFTP_REQ_HOST);
		String ftpPort = sftp.get(SFTPConstants.SFTP_REQ_PORT);
		String ftpUser = sftp.get(SFTPConstants.SFTP_REQ_USER);
		String ftpPass = sftp.get(SFTPConstants.SFTP_REQ_PASS);

		int defPort = SFTPConstants.SFTP_DEFAULT_PORT;

		if (ftpPort != null && !ftpPort.equals("")) {
			defPort = Integer.valueOf(ftpPort);
		}

		// 创建JSch对象
		JSch jsch = new JSch();

		// 根据用户名，主机ip，端口获取一个Session对象
		session = jsch.getSession(ftpUser, ftpHost, defPort);
		if (ftpPass != null) {
			// 设置密码
			session.setPassword(ftpPass);
		}
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		// 为Session对象设置properties
		session.setConfig(config);
		// 设置timeout时间
		session.setTimeout(timeout);
		// 通过Session建立链接
		session.connect();
		// 打开SFTP通道
		channel = session.openChannel("sftp");
		// 建立SFTP通道的连接
		channel.connect();
		return (ChannelSftp) channel;
	}

	public void closeChannel() throws Exception {
		if (channel != null) {
			channel.disconnect();
		}
		if (session != null) {
			session.disconnect();
		}
	}
}
