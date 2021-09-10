import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Log4j2
public class CsvConversion {
    //线程池
    static ExecutorService executor = Executors.newFixedThreadPool(16);

    public static void main(String[] args) throws InterruptedException {
        String pathname = null;
        if (pathname == null) {
            pathname = "G:\\export";
        }
        File folder = new File(pathname);
        File[] files = folder.listFiles();
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            String finalPathname = pathname;
            executor.submit(() -> {
                        try {
                            InputStreamReader reader = new InputStreamReader(
                                    new FileInputStream(file)); // 建立一个输入流对象reader
                            BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
                            File writename = new File(finalPathname + "\\done\\" + file.getName().replaceAll("xxxx", "xxxx"));
                            /* 写入Txt文件 */
                            writename.createNewFile(); // 创建新文件
                            BufferedWriter out = new BufferedWriter(new FileWriter(writename));

                            String line = "";
                            while ((line = br.readLine()) != null) {
                                //替换object
                                line = line.replaceAll("ObjectId\\((.*)\\)", "$1");
                               //替换doctorment的额外字符串
                                line = line.replaceAll("\\{\"\"\\$.*?\"\":(.*?)\\}", "$1");

                                out.write(line + System.lineSeparator());
                            }
                            out.flush(); // 把缓存区内容压入文件
                            out.close(); // 最后记得关闭文件
                            reader.close();
                            //log.info(file.delete());
                        } catch (Exception e) {
                            log.error(e);
                        }
                    }
            );
        }
        executor.shutdown();
        ThreadPoolExecutor tpe = ((ThreadPoolExecutor) executor);
        while (true) {
            int queueSize = tpe.getQueue().size();
            log.info("当前排队线程数：" + queueSize);

            int activeCount = tpe.getActiveCount();
            log.info("当前活动线程数：" + activeCount);

            long completedTaskCount = tpe.getCompletedTaskCount();
            log.info("执行完成线程数：" + completedTaskCount);
            if (executor.isTerminated()) {
                break;
            }
            Thread.sleep(3000);
        }
    }


}
