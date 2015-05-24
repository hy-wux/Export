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

import org.apache.commons.net.ftp.FTPClient;

/**
 * @author 伍鲜 2015-05-15
 */
public class FTP {

	/**
	 * FTP传送文件
	 * 
	 * @param host
	 *            FTP主机
	 * @param port
	 *            FTP端口
	 * @param user
	 *            FTP用户
	 * @param pass
	 *            FTP密码
	 * @param path
	 *            FTP路径
	 * @param file
	 *            FTP文件
	 * @param code
	 *            FTP编码
	 * @param mode
	 *            FTP模式
	 * @return
	 * @throws Exception
	 */
	public static boolean ftpFile(String host, String port, String user,
			String pass, String path, String file, String code, String mode)
			throws Exception {
		File srcFile = new File(file);
		FTPClient ftpClient = new FTPClient();
		FileInputStream fis = null;

		fis = new FileInputStream(srcFile);
		// 连接主机
		ftpClient.connect(host);
		// 登陆
		ftpClient.login(user, pass);
		// 切换路径
		ftpClient.changeWorkingDirectory(path);
		// 设置缓存
		ftpClient.setBufferSize(1024);
		// 设置编码
		ftpClient.setControlEncoding(code);
		// 设置文件类型（二进制）
		ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
		if (mode.equals("Passive")) {
			ftpClient.enterLocalPassiveMode();
		}
		// 上传文件
		ftpClient.storeFile(srcFile.getName(), fis);

		fis.close();
		ftpClient.disconnect();
		return true;
	}
}
