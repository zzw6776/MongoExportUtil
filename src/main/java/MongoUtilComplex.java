import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Log4j2
public class MongoUtilComplex {
    //线程池
    static ExecutorService executor = Executors.newFixedThreadPool(4);
    //输出地址
    static String path = "G:\\export\\";
    //文本输出地址
    static String txtPath = "txt1/";

    //不带后缀的表
    static List<String> mongoCollerticons = Arrays.asList("xxxxx");

    //带后缀的表的前缀
    static List<String> mongoCollerticonsPrefix = Arrays.asList("xxxxxx", "xxxxx", "xxxxx");

    //要排除带后缀的表
    static List<String> outMongoCollerticonsPrefix = Arrays.asList("xxxxx", "xxxxx", "xxxxx");

    //用于代码储存所有后缀的表
    static List<String> mongoCollerticonsPrefixAll = new ArrayList<>();

    //alert语句
    static Map<String, String> tableAlertString = Collections.synchronizedMap(new TreeMap<>());
    //index语句
    static Map<String, List<String>> tableIndexString = Collections.synchronizedMap(new TreeMap<>());

    //mongoexport 不带后缀语句
    static Map<String, String> mongoexport = Collections.synchronizedMap(new TreeMap<>());
    //mongoexport语句 带后缀
    static Map<String, String> mongoPrefixExport = Collections.synchronizedMap(new TreeMap<>());
    //mongo的连接池url
    static Map<String, String> mongoToUri = Collections.synchronizedMap(new TreeMap<>());
    //mongo的连接池
    static Map<String, MongoClient> mongoClients = Collections.synchronizedMap(new TreeMap<>());

    static Map<String, String> mongoTypeToMysqlType = Collections.synchronizedMap(new TreeMap<>());

    static Map<String, Boolean> collectionIsExit = Collections.synchronizedMap(new TreeMap<>());

    static Map<String, String> collectionToField = Collections.synchronizedMap(new TreeMap<>());


    static AtomicInteger subscript = new AtomicInteger(0);
    static Map<String, List<String>> mongoKeyToDateSourceString = Collections.synchronizedMap(new TreeMap<>());
    static Map<String, MongoDatabase> mongoKeyToDateSource = Collections.synchronizedMap(new TreeMap<>());

    static {

        //mongoToUri
        mongoToUri.put("rs19", "mongodb://xxx,xxx,xxx/tihuanbiaoming?replicaSet=rs19&maxPoolSize=4");
        mongoToUri.put("rs92", "mongodb://xxx,xxx,xxx/tihuanbiaoming?replicaSet=rs92&maxPoolSize=4");
        mongoToUri.put("rs84", "mongodb://xxx,xxx,xxx/tihuanbiaoming?replicaSet=rs84&maxPoolSize=4");


        mongoKeyToDateSourceString.put("rs19", Arrays.asList("xxx", "xxx"));
        mongoKeyToDateSourceString.put("rs92", Arrays.asList("xxx", "xxx"));
        mongoKeyToDateSourceString.put("rs84", Arrays.asList("xxx", "xxx"));


        setMongoClients();
        setMongoDateBases();


        //拼接带后缀的表
        for (MongoDatabase mongoDatabase : mongoKeyToDateSource.values()) {
            MongoIterable<String> listCollectionNames = mongoDatabase.listCollectionNames();
            for (String collectionName : listCollectionNames) {
                for (String collerticonsPrefix : mongoCollerticonsPrefix) {
                    if (collectionName.startsWith(collerticonsPrefix)) {
                        mongoCollerticonsPrefixAll.add(collectionName);
                    }
                }
            }
        }

        //不带后缀的表
        for (String mongoCollerticon : mongoCollerticons) {
            collectionIsExit.put(mongoCollerticon, false);
        }


        //mongoTypeToMysqlType
        mongoTypeToMysqlType.put("string", "varchar(%s) NULL,");
        mongoTypeToMysqlType.put("objectId", "varchar(%s) NOT NULL,");
        mongoTypeToMysqlType.put("object", "text NULL,");
        mongoTypeToMysqlType.put("double", "double(20, 5) NULL,");
        mongoTypeToMysqlType.put("int", "int(%s) NULL,");
        mongoTypeToMysqlType.put("long", "bigint(%s) NULL,");
        mongoTypeToMysqlType.put("date", "date NULL,");


    }

    private static void setMongoDateBases() {
        for (String key : mongoKeyToDateSourceString.keySet()) {
            MongoClient mongoClient = mongoClients.get(key);
            List<String> dabases = mongoKeyToDateSourceString.get(key);
            for (String dates : dabases) {
                mongoKeyToDateSource.put(key + ":" + dates, mongoClient.getDatabase(dates));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        Set<Map.Entry<String, MongoDatabase>> entrySet = mongoKeyToDateSource.entrySet();
        for (Map.Entry<String, MongoDatabase> mongoDatabaseEntry : entrySet) {
            String key = mongoDatabaseEntry.getKey();
            log.info("key"+key);
            MongoDatabase database = mongoDatabaseEntry.getValue();
            ArrayList<String> collectionNames = database.listCollectionNames().into(new ArrayList<>());
            //不带后缀的
            for (String collectionString : mongoCollerticons) {
                log.info(key+":"+collectionString);
                boolean isContains = collectionNames.contains(collectionString);
                if (!isContains) {
                    continue;
                }
                executor.submit(() -> {
                            try {
                                long startCollection = System.currentTimeMillis();
                                log.info("开始统计" + collectionString );
                                //确定表存在
                                collectionIsExit.put(collectionString, true);

                                MongoCollection<Document> collection = database.getCollection(collectionString);

                                //根据_id倒叙
                                List<Bson> query = getFieldBsonsQuery();
                                AggregateIterable<Document> aggregate = collection.aggregate(query);

                                //添加建表语句
                                addAlertString(collectionString, aggregate, collection);
                                //添加导出语句
                                addExportString(database, collectionString, key);
                                long endCollection = System.currentTimeMillis();
                                log.info("结束"+collectionString+":"+(endCollection-startCollection));
                            } catch (Exception e) {
                                log.error("error!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", e);
                            }
                        }

                );
            }
            //带后缀的
            for (String collectionString : mongoCollerticonsPrefixAll) {
                log.info(key+":"+collectionString);
                boolean isContains = collectionNames.contains(collectionString);
                if (!isContains) {
                    continue;
                }
                executor.submit(() -> {
                            try {
                                long startCollection = System.currentTimeMillis();
                                log.info("开始统计" + collectionString );
                                //确定表存在
                                collectionIsExit.put(collectionString, true);

                                MongoCollection<Document> collection = database.getCollection(collectionString);

                                //根据_id倒叙
                                List<Bson> query = getFieldBsonsQuery();
                                AggregateIterable<Document> aggregate = collection.aggregate(query);

                                //添加建表语句
                                addAlertString(collectionString, aggregate, collection);
                                //添加导出语句
                                addExportString(database, collectionString, key);
                                long endCollection = System.currentTimeMillis();
                                log.info("结束"+collectionString+":"+(endCollection-startCollection));
                            } catch (Exception e) {
                                log.error("error!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", e);
                            }
                        }

                );
            }

        }
        log.info("添加线程完毕");
        executor.shutdown();
        log.info("添加线程完毕");
        ThreadPoolExecutor tpe = ((ThreadPoolExecutor) executor);
        while (true) {
            int queueSize = tpe.getQueue().size();
            log.info("当前排队线程数：" + queueSize);

            int activeCount = tpe.getActiveCount();
            log.info("当前活动线程数：" + activeCount);

            long completedTaskCount = tpe.getCompletedTaskCount();
            log.info("执行完成线程数：" + completedTaskCount);
            if (executor.isTerminated()) {
                long end = System.currentTimeMillis();
                log.info("结束了！" + (end - start));
                break;
            }
            Thread.sleep(300000);
        }
        Map<String, Map> result = new TreeMap<>();
        result.put("tableAlertString", tableAlertString);
        result.put("mongoPrefixExport", mongoPrefixExport);
        result.put("mongoexport", mongoexport);
        result.put("collectionIsExit", collectionIsExit);
        result.put("tableIndexString", tableIndexString);
        for (Map.Entry<String, Map> stringMapEntry : result.entrySet()) {


            File writename = new File(path + txtPath + stringMapEntry.getKey() + ".txt");
            /* 写入Txt文件 */
            writename.createNewFile(); // 创建新文件
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            Map value = stringMapEntry.getValue();
            for (Object o : value.keySet()) {
                out.write(o.toString() + "=" + value.get(o) + System.lineSeparator());
            }
            out.close();
        }

    }

    private static List<Bson> getFieldBsonsQuery() {
        Document parse = Document.parse("{\n" +
                "                        $convert: {\n" +
                "                            input: \"$arrayofkeyvalue.v\",\n" +
                "                            to: \"string\",\n" +
                "                            onError: \"hahaha\",\n" +
                "                            onNull:\"\"\n" +
                "                        }\n" +
                "                    }");
        return Arrays.asList(
                project(computed("arrayofkeyvalue", eq("$objectToArray", "$$ROOT"))),
                unwind("$arrayofkeyvalue"),
                group(computed("_id", "$arrayofkeyvalue.k"),
                        addToSet("type", computed("$type", "$arrayofkeyvalue.v")),
                        max("lenth", computed("$strLenCP", parse))
                )

        );
    }

    public static String fillString(int num, int digit) {
        /**
         * 0：表示前面补0
         * digit：表示保留数字位数
         * d：表示参数为正数类型
         */
        String format = String.format("%0" + digit + "d", num);
        return format;
    }

    private static void addExportString(MongoDatabase database, String collectionString,  String key) {
        String fieldsString = getFieldsString(collectionString);
        StringBuffer exportString = new StringBuffer();
        String url = mongoToUri.get(key.substring(0, 4)).replaceAll("tihuanbiaoming", database.getName()).replaceAll("(\\?|&)maxPoolSize=[0-9]*", "");
        exportString.append("mongoexport ").append("--uri=").append(url).append(" ");
        Boolean isPrefix = isPrefix(collectionString);
        String filename;
        if (isPrefix) {
            filename = getPrefixByCollectingString(collectionString) + "." + fillString(subscript.addAndGet(1), 6);
        } else {
            filename = collectionString;
        }


        exportString.append("-c ").append(collectionString).append(" --type=csv -o " + path + "xxxx.").append(filename).append(".csv -f ").append(fieldsString);
        exportString.append(" --limit=10000 ");
        //export构建完成
        if (isPrefix) {
            String mongoPrefixExportString = MongoUtilComplex.mongoPrefixExport.get(collectionString);
            if (mongoPrefixExportString == null || mongoPrefixExportString.length() < exportString.toString().length()) {
                mongoPrefixExport.put(collectionString, exportString.toString());
            }
        } else {
            String mongoExportString = mongoexport.get(collectionString);
            if (mongoExportString == null || mongoExportString.length() < exportString.toString().length()) {
                mongoexport.put(collectionString, exportString.toString());
            }
        }
    }



    private static Boolean isPrefix(String collectionString) {
        Boolean isPrefix = false;
        for (String collerticonsPrefix : mongoCollerticonsPrefix) {
            if (collectionString.startsWith(collerticonsPrefix) && !outMongoCollerticonsPrefix.contains(collectionString)) {
                isPrefix = true;
            }
        }
        return isPrefix;
    }

    private static void addAlertString(String collectionString, AggregateIterable<Document> documents, MongoCollection<Document> collection) {
        collectionString = getPrefixByCollectingString(collectionString);
//        if (collectionToField.keySet().contains(collectionString)) {
//            return;
//        }

        StringBuffer fieldsString = new StringBuffer();
        StringBuffer alertString = new StringBuffer();
        alertString.append("CREATE TABLE `").append(collectionString).append("` (");
        alertString.append("`ti_id` bigint(50) NOT NULL AUTO_INCREMENT,");
        for (Document document : documents) {
            fieldsString.append(document.get("_id", Document.class).get("_id").toString()).append(",");

            Object types = document.get("type");
            String typeString;
            if (types instanceof Collection) {
                List<String> typesList = (List) types;
                typesList.remove("null");
                if (typesList.size() == 1) {
                    typeString = typesList.get(0);
                } else if (typesList.contains("object")) {
                    typeString = "object";
                } else if (typesList.contains("objectId")) {
                    typeString = "objectId";
                } else if (typesList.contains("string")) {
                    typeString = "string";
                } else if (typesList.contains("double")) {
                    typeString = "double";
                } else if (typesList.contains("long")) {
                    typeString = "long";
                } else if (typesList.contains("int")) {
                    typeString = "int";
                } else if (typesList.contains("date")) {
                    typeString = "date";
                } else if (typesList.size()==0) {
                    typeString = null;
                } else {
                    throw new RuntimeException(typesList.toString());
                }
            } else {
                typeString = types.toString();
            }
            if (typeString == null || typeString.equals("null")) {
                typeString = "string";
            }
            String type = mongoTypeToMysqlType.get(typeString);
            Integer lenth = document.get("lenth", Integer.class);
            if (lenth == 0) {
                lenth = 100;
            }
            type = String.format(type, lenth);
            alertString.append("`").append(document.get("_id", Document.class).get("_id", String.class))
                    .append("`").append(" ").append(type);
        }

        alertString.append("PRIMARY KEY (`ti_id`));");


        //alert构建完成
        String alertResultString = tableAlertString.get(collectionString);
        if (alertResultString == null || alertResultString.length() < alertString.toString().length()) {
            collectionToField.put(collectionString, fieldsString.substring(0, fieldsString.length() - 1));
            tableAlertString.put(collectionString, alertString.toString());
        }

        List<String> indexLists = new ArrayList<>();
        ListIndexesIterable<Document> indexs = collection.listIndexes();
        for (Document document : indexs) {
            String name = document.get("name").toString();
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("ALTER TABLE `" + collectionString + "` ").append("ADD ");
            Object unique = document.get("unique");
            if ("true".equals(unique) || "_id_".equals(name)) {
                stringBuffer.append("UNIQUE ");
            }
            stringBuffer.append("INDEX ");
            if (name.length() > 50) {
                name = name.substring(0, 50);
            }
            stringBuffer.append("`").append(name).append("`(");
            Map key = (Map) document.get("key");
            Set set = key.keySet();
            List<String> keys = new ArrayList<>();
            for (Object o : set) {
                keys.add("`" + o + "`");
            }
            String join = String.join(",", keys);
            stringBuffer.append(join).append(");");
            indexLists.add(stringBuffer.toString());
        }
        List<String> indexString = tableIndexString.get(collectionString);
        if (indexString != null) {
            int length = String.join(",", indexString).length();
            int length1 = String.join(",", indexLists).length();
            if (length1 > length) {
                tableIndexString.put(collectionString, indexLists);
            }
        } else {
            tableIndexString.put(collectionString, indexLists);
        }

    }

    private static String getPrefixByCollectingString(String collectionString) {
        for (String collerticonsPrefix : mongoCollerticonsPrefix) {
            if (collectionString.startsWith(collerticonsPrefix) && !outMongoCollerticonsPrefix.contains(collectionString)) {
                collectionString = collerticonsPrefix;
            }
        }
        return collectionString;
    }



    private static String getFieldsString(String mongoCollerticon) {
        mongoCollerticon = getPrefixByCollectingString(mongoCollerticon);
        String field = collectionToField.get(mongoCollerticon);
        if (field == null
        ) {
            throw new RuntimeException(mongoCollerticon + "字段不存在");
        }
        return field;
    }





    private static void setMongoClients() {
        Set<String> keySet = mongoToUri.keySet();
        for (String key : keySet) {
            MongoClientSettings build = MongoClientSettings.builder()
                    .applyToSocketSettings(builder -> {
                        builder.connectTimeout(18000000, MILLISECONDS);
                        builder.readTimeout(18000000, MILLISECONDS);
                    })
                    .applyToClusterSettings(builder -> builder.serverSelectionTimeout(18000000, MILLISECONDS))
                    .applyConnectionString(new ConnectionString(mongoToUri.get(key)))
                    .build();
            mongoClients.put(key, MongoClients.create(build));
            // mongoClients.put(key, MongoClients.create(mongoToUri.get(key)));
        }

    }

}
