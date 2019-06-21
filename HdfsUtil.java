
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.jcraft.jsch.Buffer;

/**
 * dgunify
 *HDFSUri = hdfs://127.0.0.1:9000
 */
public class HdfsUtil {

	/**
	 * 获取文件系统
	 *
	 * @return FileSystem 文件系统
	 */
	public static FileSystem getFileSystem(String HDFSUri) {
		// 读取配置文件
		Configuration conf = new Configuration();
		// 文件系统
		FileSystem fs = null;
		String hdfsUri = HDFSUri;
		if (StringUtils.isBlank(hdfsUri)) {
			// 返回默认文件系统 如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
			try {
				URI uri = new URI(hdfsUri.trim());
				fs = FileSystem.get(uri, conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fs;
	}

	/**
	 * 创建文件目录
	 *
	 * @param path 文件路径
	 */
	public static void mkdir(String path,String HDFSUri) {
		try {
			FileSystem fs = getFileSystem(HDFSUri);
			System.out.println("FilePath=" + path);
			// 创建目录
			fs.mkdirs(new Path(path));
			// 释放资源
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 判断目录是否存在
	 *
	 * @param filePath 目录路径
	 * @param create   若不存在是否创建
	 */
	public static boolean existDir(String filePath, boolean create,String HDFSUri) {
		boolean flag = false;

		if (StringUtils.isEmpty(filePath)) {
			return flag;
		}

		try {
			Path path = new Path(filePath);
			// FileSystem对象
			FileSystem fs = getFileSystem(HDFSUri);

			if (create) {
				if (!fs.exists(path)) {
					fs.mkdirs(path);
				}
			}

			if (fs.isDirectory(path)) {
				flag = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return flag;
	}

	/**
	 * 本地文件上传至 HDFS
	 *
	 * @param srcFile  源文件 路径
	 * @param destPath hdfs路径
	 */
	public static void copyFileToHDFS(String srcFile, String destPath,String HDFSUri) throws Exception {

		FileInputStream fis = new FileInputStream(new File(srcFile));// 读取本地文件
		Configuration config = new Configuration();
		System.out.print("HDFSUri + destPath="+HDFSUri + destPath);
		FileSystem fs = FileSystem.get(URI.create(HDFSUri + destPath), config);
		OutputStream os = fs.create(new Path(destPath));
		// copy
		IOUtils.copyBytes(fis, os, 4096, true);
		System.out.println("copy ok...");
		fs.close();
	}
	
	/**
	* 移动hdfs上目录或文件到
	* @param path 当前路径
	* @param newPath 目标路径
	* @return 是否移动成功
	*/
	public static boolean mv(String HDFSUri,String path,String newPath) {
		boolean result = false;
		FileSystem fs = null;
		try {
			fs =  getFileSystem(HDFSUri);
			if (!fs.exists(new Path(newPath))){
				result=fs.rename(new Path(path),new Path(newPath)); 
			}else {
				//LOGGER.warn("HDFS上目录： {} 被占用！",newPath);
			}
		} catch (Exception e) {
			//LOGGER.error("移动HDFS上目录：{} 失败！", path, e);
		} finally {
			//close(fs);
		}

	   return result;
	}
	
	/**
	 * 文件上传至 HDFS
	 *
	 * @param srcFile  源文件 路径
	 * @param destPath hdfs路径
	 */
	public static void uploadFileToHDFS(InputStream fis, String destPath,String filename,String HDFSUri) throws Exception {

		
		
		try {
			// 验证是否存在
			/*
			 * if(!existDir(destPath, false,HDFSUri)) { mkdir(destPath,HDFSUri);
			 * System.out.println("mkdir ok"); }
			 */
			//FileInputStream fis = new FileInputStream(new File(srcFile));// 读取本地文件
			Configuration config = new Configuration();
			System.out.println("HDFSUri + destPath + filenam="+HDFSUri + destPath+"/"+filename);	
			FileSystem fs = FileSystem.get(URI.create(HDFSUri + destPath+"/"+filename), config);
			OutputStream os = fs.create(new Path(destPath+"/"+filename));
			// copy
			IOUtils.copyBytes(fis, os, 4096, true);
			System.out.println("copy ok...");
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
	}

	/**
	 * 从 HDFS 下载文件到本地
	 *
	 * @param srcFile  HDFS文件路径
	 * @param destPath 本地路径
	 */
	public static void getFile(String srcFile, String destPath,String HDFSUri) throws Exception {
		// hdfs文件 地址
		String file = HDFSUri + srcFile;
		Configuration config = new Configuration();
		// 构建FileSystem
		FileSystem fs = FileSystem.get(URI.create(file), config);
		// 读取文件
		InputStream is = fs.open(new Path(file));
		IOUtils.copyBytes(is, new FileOutputStream(new File(destPath)), 2048, true);// 保存到本地 最后 关闭输入输出流
		System.out.println("downlaod ok...");
		fs.close();
	}

	/**
	 * 删除文件或者文件目录
	 *
	 * @param path
	 */
	public static void rmdir(String path,String HDFSUri) {
		try {
			// 返回FileSystem对象
			FileSystem fs = getFileSystem(HDFSUri);

			String hdfsUri = HDFSUri;
			if (StringUtils.isNotBlank(hdfsUri)) {
				path = hdfsUri + path;
			}
			System.out.println("path:" + path);
			// 删除文件或者文件目录 delete(Path f) 此方法已经弃用
			System.out.println(fs.delete(new Path(path), true));

			// 释放资源
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除文件
	 *
	 * @param path
	 */
	public static void deleteFile(String path,String HDFSUri) {
		try {
			// 返回FileSystem对象
			FileSystem fs = getFileSystem(HDFSUri);
			
			System.out.println("path:" + path);
			// 删除文件或者文件目录 delete(Path f) 此方法已经弃用
			System.out.println(fs.delete(new Path(path), true));
			
			// 释放资源
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取文件的内容
	 * 
	 * @param filePath
	 * @throws IOException
	 */
	public static void readFile(String filePath,String HDFSUri) throws IOException {
		Configuration config = new Configuration();
		String file = HDFSUri + filePath;
		FileSystem fs = FileSystem.get(URI.create(file), config);
		// 读取文件
		InputStream is = fs.open(new Path(file));
		// 读取文件
		IOUtils.copyBytes(is, System.out, 2048, false); // 复制到标准输出流
		fs.close();
	}
	
	/**
	 * 遍历指定目录(direPath)下的所有文件
	 */
	public static List<Map<String,String>> getDirectoryFromHdfs(String direPath) throws Exception {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(direPath), config);
		FileStatus[] filelist = fs.listStatus(new Path(direPath));
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		for (int i = 0; i < filelist.length; i++) {
			FileStatus fileStatus = filelist[i];
			Map<String,String> map = new HashMap<String, String>();
			map.put("url", fileStatus.getPath().toString());
			map.put("index", i+1+"");
			map.put("name", fileStatus.getPath().getName());
			map.put("size", UnitUtil.getPrintSize(fileStatus.getLen())+"");
			map.put("group", fileStatus.getGroup());
			map.put("owner", fileStatus.getOwner());
			map.put("isDirectory", fileStatus.isDirectory()+"");
			map.put("isFile", fileStatus.isFile()+"");
			TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm",Locale.SIMPLIFIED_CHINESE); 
			String date = sdf.format(new Date(fileStatus.getModificationTime()));
			map.put("date", date);
			list.add(map);
			
		}
		fs.close();
		return list;
	}
	
	/**
	 * 下载文件
	 */
	public static byte [] downLoadFileFromHDFS(String src) throws Exception {
		/*
		 * Configuration config = new Configuration(); FileSystem fs =
		 * FileSystem.get(config); Path srcPath = new Path(src); InputStream in =
		 * fs.open(srcPath);
		 */
		
		Configuration config = new Configuration();
		String file = src;
		FileSystem fs = FileSystem.get(URI.create(file), config);
		// 读取文件
		InputStream in = fs.open(new Path(file));
		
		try {
			// 将文件COPY到标准输出(即控制台输出)
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, out, 4096, false);
			return out.toByteArray();
		} finally {
			IOUtils.closeStream(in);
			fs.close();
		}
	}


	public static void main(String[] args) throws Exception {
		//String HDFSFile = "/hollycrm/data01/codecs/1月.zip";
		//String localFile = "D:\\1月.zip";
		// 连接fs
		FileSystem fs = getFileSystem("xxx");
		//System.out.println(fs.getUsed());
		// 创建路径
		//mkdir("/spark-demo");
		// 验证是否存在
		//System.out.println(existDir("/spark-demo", false));
		// 上传文件到HDFS
		//copyFileToHDFS("D:\\workspace\\spark-demo2\\target\\spark-demo2-0.0.1-SNAPSHOT.jar", "/spark-demo/spark-demo2-0.0.1-SNAPSHOT.jar");
		//copyFileToHDFS("F:\\words.txt", "/spark-demo/words.txt");
		// 下载文件到本地
		//getFile("/zhaojy/HDFSTest.txt", "D:\\HDFSTest.txt");
		// getFile(HDFSFile,localFile);
		// 删除文件
		//rmdir("/spark-demo/out-1553524260785.txt");
		// 读取文件
		readFile("/spark-demo/export/1553633629669","xxx");
	}
}
