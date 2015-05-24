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
package com.petasoft.export.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.petasoft.export.exp.ExpData;
import com.petasoft.export.ftp.FtpData;
import com.petasoft.export.meg.MegData;
import com.petasoft.export.util.FileUtil;
import com.petasoft.export.util.PubFun;

/**
 * 读取配置文件，完成程序的初始化配置。
 * 
 * @author 伍鲜 2015-05-15
 */
public class Configuration extends HashMap<Object, String> {
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(Configuration.class);

	/**
	 * 处理日期
	 */
	public String tranDate = PubFun.dateHFormat.format(new Date());
	/**
	 * 短格式日期
	 */
	public String shortDate = PubFun.shortFormat.format(new Date());

	/**
	 * 开始时间
	 */
	public String beginTranTime;
	/**
	 * 结束时间
	 */
	public String endTranTime;

	/**
	 * 配置文件
	 */
	public String default_site = "conf/core-default.xml";
	public String core_site = "conf/core-site.xml";
	public String db_site = "conf/db-site.xml";
	public String export_site = "conf/export-site.xml";
	public String merge_site = "conf/merge-site.xml";
	public String ftp_site = "conf/ftp-site.xml";

	public String EXP_STATUS_INI = "";
	public String EXP_STATUS_ERR = "";
	public String MEG_STATUS_INI = "";
	public String MEG_STATUS_ERR = "";
	public String FTP_STATUS_INI = "";
	public String FTP_STATUS_ERR = "";

	/**
	 * key: intercode<br />
	 * value: srctable|incrflag
	 */
	public HashMap<String, String> interMap = new HashMap<String, String>();
	/**
	 * key: intercode<br />
	 * value: subintercodes
	 */
	public HashMap<String, String> mergeMap = new HashMap<String, String>();
	/**
	 * key: intercode<br />
	 */
	public HashMap<String, String> ftpInter = new HashMap<String, String>();

	/**
	 * 导出列表<br />
	 * key: intercode|srctable|incrflag<br />
	 * value: time
	 */
	public Map<String, Integer> expMap = new HashMap<String, Integer>();
	public List<ExpData> expList = new ArrayList<ExpData>();
	/**
	 * key: intercode|srctable|incrflag<br />
	 * value: time
	 */
	public Map<String, Integer> expSucc = new HashMap<String, Integer>();
	/**
	 * 当前正在导出的个数
	 */
	public int expCount = 0;

	/**
	 * 合并列表<br />
	 * key: intercode<br />
	 * value: subcode,subcode,subcode
	 */
	public Map<String, String> megMap = new HashMap<String, String>();
	public List<MegData> megList = new ArrayList<MegData>();
	/**
	 * key: intercode|srctable|incrflag
	 */
	public Map<String, Integer> megSucc = new HashMap<String, Integer>();

	public boolean ftp = false;
	/**
	 * 传输列表<br />
	 * key: intercode|srctable|incrflag<br />
	 * value: time
	 */
	public Map<String, Integer> ftpMap = new HashMap<String, Integer>();
	public List<FtpData> ftpList = new ArrayList<FtpData>();
	/**
	 * key: intercode|srctable|incrflag<br />
	 * value: time
	 */
	public Map<String, Integer> ftpSucc = new HashMap<String, Integer>();

	/**
	 * 加载配置文件
	 */
	public void load() {
		try {
			load(default_site);
			load(core_site);
			load(db_site);

			put("export.core.procDir.String", get("export.core.procDir"));
			put("export.core.logsDir.String", get("export.core.logsDir"));
			put("export.core.dataDir.String", get("export.core.dataDir"));
			put("export.core.sqlsDir.String", get("export.core.sqlsDir"));

			put("export.core.procDir",
					PubFun.replaceDate(get("export.core.procDir"), tranDate));
			put("export.core.logsDir",
					PubFun.replaceDate(get("export.core.logsDir"), tranDate));
			put("export.core.dataDir",
					PubFun.replaceDate(get("export.core.dataDir"), tranDate));
			put("export.core.sqlsDir",
					PubFun.replaceDate(get("export.core.sqlsDir"), tranDate));

			createDir();
			logger.info("proc dir: " + get("export.core.procDir"));
			logger.info("logs dir: " + get("export.core.logsDir"));
			logger.info("data dir: " + get("export.core.dataDir"));
			logger.info("sqls dir: " + get("export.core.sqlsDir"));

			load(ftp_site);

			loadExport(export_site);
			loadMerge(merge_site);

			mergeMap.putAll(megMap);
			ftpInter.putAll(interMap);
			for (String key : mergeMap.keySet()) {
				ftpInter.remove(key);
				for (String inter : mergeMap.get(key)
						.split(PubFun.comSeparator)) {
					ftpInter.remove(inter);
				}
			}
			ftpInter.putAll(mergeMap);

			EXP_STATUS_INI = get("export.core.procDir") + "/" + shortDate + "_"
					+ get("export.core.SystemCode") + "_ExpStatus" + ".ini";
			EXP_STATUS_ERR = get("export.core.procDir") + "/" + shortDate + "_"
					+ get("export.core.SystemCode") + "_ExpStatus" + ".err";

			MEG_STATUS_INI = get("export.core.procDir") + "/" + shortDate + "_"
					+ get("export.core.SystemCode") + "_MegStatus" + ".ini";
			MEG_STATUS_ERR = get("export.core.procDir") + "/" + shortDate + "_"
					+ get("export.core.SystemCode") + "_MegStatus" + ".err";

			FTP_STATUS_INI = get("export.core.procDir") + "/" + shortDate + "_"
					+ get("export.core.SystemCode") + "_FtpStatus" + ".ini";
			FTP_STATUS_ERR = get("export.core.procDir") + "/" + shortDate + "_"
					+ get("export.core.SystemCode") + "_FtpStatus" + ".err";

			override(EXP_STATUS_INI);
			override(EXP_STATUS_ERR);
			override(MEG_STATUS_INI);
			override(MEG_STATUS_ERR);
			override(FTP_STATUS_INI);
			override(FTP_STATUS_ERR);
		} catch (Exception e) {
			logger.error(PubFun.getExceptionString(e));
		}
	}

	private void override(String filename) throws Exception {
		File file = new File(filename);
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
	}

	/**
	 * 加载配置文件信息
	 * 
	 * @param filename
	 *            配置文件名称
	 * @throws DocumentException
	 */
	private void load(String filename) throws DocumentException {
		File file = new File(filename);
		SAXReader reader = new SAXReader();
		Document doc = reader.read(file);
		Element configuration = doc.getRootElement();
		Element property;
		for (Iterator<?> i = configuration.elementIterator("property"); i
				.hasNext();) {
			property = (Element) i.next();
			put(property.elementText("name"), property.elementText("value"));
		}
	}

	private void loadExport(String filename) throws Exception {
		File file = new File(filename);
		SAXReader reader = new SAXReader();
		Document doc;
		doc = reader.read(file);
		Element configuration = doc.getRootElement();
		Element property;
		for (Iterator<?> i = configuration.elementIterator("property"); i
				.hasNext();) {
			property = (Element) i.next();
			String intecode = property.elementText("intecode");
			String srctable = property.elementText("srctable");
			String incrflag = property.elementText("incrflag");
			String expflg = property.elementText("expflg");
			String expsql = property.elementText("expsql");
			String chkflg = property.elementText("chkflg");
			String chksql = property.elementText("chksql");
			if (expflg.equalsIgnoreCase("true") && !expsql.equals("")) {
				addScriptFile(intecode, srctable, incrflag, expsql);
			}
			if (chkflg.equalsIgnoreCase("true") && !chksql.equals("")) {
				addChecksFile(intecode, srctable, incrflag, chksql);
			}
		}
	}

	private void loadMerge(String filename) throws DocumentException {
		File file = new File(filename);
		SAXReader reader = new SAXReader();
		Document doc = reader.read(file);
		Element configuration = doc.getRootElement();
		Element property;
		for (Iterator<?> i = configuration.elementIterator("property"); i
				.hasNext();) {
			property = (Element) i.next();
			megMap.put(property.elementText("intecode"),
					property.elementText("subscode"));
		}
	}

	private void createDir() {
		logger.info("begin create director.");
		File file = null;
		file = new File(get("export.core.logsDir"), "");
		file.mkdirs();
		file = new File(get("export.core.procDir"), "");
		file.mkdirs();
		file = new File(get("export.core.dataDir"), "");
		file.mkdirs();
		file = new File(get("export.core.sqlsDir"), "");
		file.mkdirs();
		logger.info("success create director.");
	}

	private void addScriptFile(String intercode, String srctable,
			String incrflag, String expsql) throws Exception {

		String fileName = get("export.core.sqlsDir") + "/" + intercode + ".sql";
		logger.info("prepared export sql file: " + fileName);

		FileUtil.overrid(fileName, expsql);

		String key = String.format("%s|%s|%s", new Object[] { intercode,
				srctable, incrflag });
		expMap.put(key, 1);
		if (interMap.containsKey(intercode)) {
			throw new Exception("duplicate intercode: " + intercode);
		} else {
			interMap.put(intercode,
					String.format("%s|%s", new Object[] { srctable, incrflag }));
		}
	}

	private void addChecksFile(String intercode, String srctable,
			String incrflag, String chksql) throws Exception {

		intercode = intercode + "_@" + get("export.core.QualityCode");

		String fileName = get("export.core.sqlsDir") + "/" + intercode + ".sql";
		logger.info("prepared check sql file: " + fileName);

		FileUtil.overrid(fileName, chksql);

		String key = String.format("%s|%s|%s", new Object[] { intercode,
				srctable, incrflag });
		expMap.put(key, 1);
		if (interMap.containsKey(intercode)) {
			throw new Exception("duplicate intercode: " + intercode);
		} else {
			interMap.put(intercode,
					String.format("%s|%s", new Object[] { srctable, incrflag }));
		}
		if (megMap.containsKey(get("export.core.QualityCode"))) {
			megMap.put(get("export.core.QualityCode"),
					megMap.get(get("export.core.QualityCode")).concat(",")
							.concat(intercode));
		} else {
			megMap.put(get("export.core.QualityCode"), intercode);
		}
	}

	@Override
	public String get(Object key) {
		if (!containsKey(key)) {
			put(key, "");
		}
		return super.get(key);
	}

	public static void main(String[] args) {
	}
}