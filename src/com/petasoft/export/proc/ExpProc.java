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
package com.petasoft.export.proc;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import com.petasoft.export.conf.Configuration;
import com.petasoft.export.util.ftp.FTP;
import com.petasoft.export.util.ftp.SFTP;
import com.petasoft.export.util.zip.Gzip;

/**
 * 打包配置文件、主控日志、SQL查询脚本、（卸数、合并、传输）日志文件；<br/>
 * 将打包后的文件压缩后发送到指定的服务器。
 * 
 * @author 伍鲜 2015-05-15
 */
public class ExpProc {
	private Configuration conf;
	private String basepath = "/";
	private File dir;

	private String tar;
	private String ok;

	public ExpProc(Configuration conf) {
		this.conf = conf;
		tar = getProcFile("tar");
		ok = getProcFile("ok");
	}

	private String getProcFile(String name) {
		return conf.get("export.core.dataDir") + "/" + conf.shortDate + "_"
				+ conf.get("export.core.SystemCode") + "_Export_Proc." + name;
	}

	private void addArchive(File file, TarArchiveOutputStream aos,
			String basepath) throws Exception {
		if (file.exists()) {
			TarArchiveEntry entry = new TarArchiveEntry(basepath + "/"
					+ file.getName());
			entry.setSize(file.length());
			aos.putArchiveEntry(entry);
			BufferedInputStream bis = new BufferedInputStream(
					new FileInputStream(file));
			int count;
			byte data[] = new byte[1024];
			while ((count = bis.read(data, 0, data.length)) != -1) {
				aos.write(data, 0, count);
			}
			bis.close();
			aos.closeArchiveEntry();
		}
	}

	private File archive() throws Exception {
		File file = new File(tar);
		TarArchiveOutputStream aos = new TarArchiveOutputStream(
				new FileOutputStream(file));

		// 压缩配置文件
		addArchive(new File(conf.default_site), aos, basepath);
		addArchive(new File(conf.core_site), aos, basepath);
		addArchive(new File(conf.db_site), aos, basepath);
		addArchive(new File(conf.export_site), aos, basepath);
		addArchive(new File(conf.merge_site), aos, basepath);
		addArchive(new File(conf.ftp_site), aos, basepath);

		// 压缩主控文件
		addArchive(new File("proc/ExportProc.log"), aos, basepath);
		addArchive(new File(conf.EXP_STATUS_INI), aos, basepath);
		addArchive(new File(conf.EXP_STATUS_ERR), aos, basepath);
		addArchive(new File(conf.MEG_STATUS_INI), aos, basepath);
		addArchive(new File(conf.MEG_STATUS_ERR), aos, basepath);
		addArchive(new File(conf.FTP_STATUS_INI), aos, basepath);
		addArchive(new File(conf.FTP_STATUS_ERR), aos, basepath);

		// 压缩日志文件
		dir = new File(conf.get("export.core.logsDir"));
		File[] logs = dir.listFiles();
		for (File log : logs) {
			if (log.getName().substring(0, 8).equals(conf.shortDate)
					&& log.getName().endsWith(".log")) {
				addArchive(log, aos, basepath);
			}
		}

		// 压缩脚本文件
		dir = new File(conf.get("export.core.sqlsDir"));
		File[] sqls = dir.listFiles();
		for (File sql : sqls) {
			if (sql.getName().endsWith(".sql")) {
				addArchive(sql, aos, basepath);
			}
		}
		return file;
	}

	public void ftpProcLog() throws Exception {
		File file = Gzip.gzip(archive(),
				new File(conf.get("export.core.dataDir")), true);
		File OK = new File(ok);
		if (OK.exists()) {
			OK.delete();
		}
		OK.createNewFile();
		if (conf.get("export.ftp.type").equals("SFTP")) {
			SFTP.ftpFile(conf.get("export.ftp.host"),
					conf.get("export.ftp.port"), conf.get("export.ftp.user"),
					conf.get("export.ftp.pass"), conf.get("export.ftp.path"),
					conf.get("export.core.dataDir") + "/" + file.getName(),
					conf.get("export.core.CharSet"),
					conf.get("export.ftp.timeout"));
			SFTP.ftpFile(conf.get("export.ftp.host"),
					conf.get("export.ftp.port"), conf.get("export.ftp.user"),
					conf.get("export.ftp.pass"), conf.get("export.ftp.path"),
					conf.get("export.core.dataDir") + "/" + OK.getName(),
					conf.get("export.core.CharSet"),
					conf.get("export.ftp.timeout"));
		} else {
			FTP.ftpFile(conf.get("export.ftp.host"),
					conf.get("export.ftp.port"), conf.get("export.ftp.user"),
					conf.get("export.ftp.pass"), conf.get("export.ftp.path"),
					conf.get("export.core.dataDir") + "/" + file.getName(),
					conf.get("export.core.CharSet"),
					conf.get("export.ftp.mode"));
			FTP.ftpFile(conf.get("export.ftp.host"),
					conf.get("export.ftp.port"), conf.get("export.ftp.user"),
					conf.get("export.ftp.pass"), conf.get("export.ftp.path"),
					conf.get("export.core.dataDir") + "/" + OK.getName(),
					conf.get("export.core.CharSet"),
					conf.get("export.ftp.mode"));
		}
	}

	public static void main(String[] args) {
	}
}
