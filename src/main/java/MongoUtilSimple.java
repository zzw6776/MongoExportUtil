import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
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

import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Log4j2
public class MongoUtilSimple {
    //线程池
    static ExecutorService executor = Executors.newFixedThreadPool(4);
    //输出地址
    static String path = "G:\\export\\";
    //文档储存地址
    static String txtPath = "txt1\\";
    //需要导出的表
    static List<String> mongoCollerticons = Arrays.asList("xxxx");
    //全部类型
    static HashSet<String> types = new HashSet<>();
    //alert语句
    static Map<String, String> tableAlertString = Collections.synchronizedMap(new TreeMap<>());
    //alertIndex语句
    static Map<String, List<String>> tableIndexString = Collections.synchronizedMap(new TreeMap<>());

    //mongoexport 不带后缀语句
    static Map<String, String> mongoexport = Collections.synchronizedMap(new TreeMap<>());
    //mongo的连接池url
    static Map<String, String> mongoToUri = Collections.synchronizedMap(new TreeMap<>());
    //mongo的连接池
    static Map<String, MongoClient> mongoClients = Collections.synchronizedMap(new TreeMap<>());

    static Map<String, String> mongoTypeToMysqlType = Collections.synchronizedMap(new TreeMap<>());

    static Map<String, Boolean> collectionIsExit = Collections.synchronizedMap(new TreeMap<>());

    static Map<String, String> collectionToField = Collections.synchronizedMap(new TreeMap<>());



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


        //mongoTypeToMysqlType
        mongoTypeToMysqlType.put("string", "varchar(%s) NULL,");
        mongoTypeToMysqlType.put("objectId", "varchar(%s) NOT NULL,");
        mongoTypeToMysqlType.put("object", "text NULL,");
        mongoTypeToMysqlType.put("double", "double(%s, 2) NULL,");
        mongoTypeToMysqlType.put("int", "int(%s) NULL,");
        mongoTypeToMysqlType.put("long", "bigint(%s) NULL,");
        mongoTypeToMysqlType.put("date", "date NULL,");

        for (String mongoCollerticon : mongoCollerticons) {
            collectionIsExit.put(mongoCollerticon, false);
        }

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
            MongoDatabase database = mongoDatabaseEntry.getValue();
            ArrayList<String> collectionNames = database.listCollectionNames().into(new ArrayList<>());
            for (String collectionString : mongoCollerticons) {
                boolean isContains = collectionNames.contains(collectionString);
                if (!isContains) {
                    continue;
                }
                log.info(key + ":" + collectionString);
                executor.submit(() -> {
                            try {
                                long startCollection = System.currentTimeMillis();
                                log.info("开始统计" + collectionString);
                                //确定表存在
                                collectionIsExit.put(collectionString, true);

                                MongoCollection<Document> collection = database.getCollection(collectionString);

                                //根据_id倒叙
                                Document parse = Document.parse("{\n" +
                                        "                        $convert: {\n" +
                                        "                            input: \"$arrayofkeyvalue.v\",\n" +
                                        "                            to: \"string\",\n" +
                                        "                            onError: \"hahaha\",\n" +
                                        "                            onNull:\"\"\n" +
                                        "                        }\n" +
                                        "                    }");
                                List<Bson> query = Arrays.asList(
                                        project(computed("arrayofkeyvalue", eq("$objectToArray", "$$ROOT"))),
                                        unwind("$arrayofkeyvalue"),
                                        group(computed("_id", "$arrayofkeyvalue.k"),
                                                addToSet("type", computed("$type", "$arrayofkeyvalue.v")),
                                                max("lenth", computed("$strLenCP", parse))
                                        )

                                );
                                AggregateIterable<Document> aggregate = collection.aggregate(query);

                                addJavaTypes(aggregate);
                                //添加建表语句
                                addAlertString(collectionString, aggregate, collection);
                                //添加导出语句
                                addExportString(database, collectionString, aggregate, key);
                                long endCollection = System.currentTimeMillis();
                                log.info("结束" + collectionString + ":" + (endCollection - startCollection));
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


    private static void addExportString(MongoDatabase database, String collectionString, AggregateIterable<Document> next, String key) {
        String fieldsString = getFieldsString(collectionString);
        StringBuffer exportString = new StringBuffer();
        String url = mongoToUri.get(key.substring(0, 4)).replaceAll("tihuanbiaoming", database.getName()).replaceAll("(\\?|&)maxPoolSize=4", "");
        exportString.append("mongoexport ").append("--uri=").append(url).append(" ");
        String filename = collectionString;


        exportString.append("-c ").append(collectionString).append(" --type=csv -o " + path + "xxx.").append(filename).append(".csv -f ").append(fieldsString);
        //exportString.append(" --limit=10000 ");
        //export构建完成
        mongoexport.put(collectionString, exportString.toString());
    }


    private static void addAlertString(String collectionString, AggregateIterable<Document> documents, MongoCollection<Document> collection) {

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
                } else if (typesList.size() == 0) {
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
        collectionToField.put(collectionString, fieldsString.substring(0, fieldsString.length() - 1));
        tableAlertString.put(collectionString, alertString.toString());

        //构建index
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

        tableIndexString.put(collectionString, indexLists);

    }


    private static void addJavaTypes(AggregateIterable<Document> documents) {
        for (Document document : documents) {
            Object documentTypes = document.get("type");
            if (documentTypes instanceof Collection) {
                types.addAll(new HashSet<>((List) documentTypes));
            } else {
                types.add(documentTypes.toString());
            }

        }

    }

    private static String getFieldsString(String mongoCollerticon) {
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
        }

    }

}
