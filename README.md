# MongoExportUtil
### 该项目能做什么

- 获取mongo最多字段并生成mysql的建表语句

- 获取mongo表的索引并生成mysql的索引语句

- 根据mongo表生成mongoexport语句



### 实战案例

https://www.cnblogs.com/MRLL/p/15250758.html


### 该项目怎么用

#### 参数解释

```Java
//输出地址
static String path = "G:\\export\\";
//文档储存地址
static String txtPath = "txt1\\";
//需要导出的表
static List<String> mongoCollerticons = Arrays.asList("xxxx");


//以下为带后缀表的参数
//带后缀的表的前缀
static List<String> mongoCollerticonsPrefix = Arrays.asList("xxxxxx", "xxxxx", "xxxxx");

//要排除带后缀的表
static List<String> outMongoCollerticonsPrefix = Arrays.asList("xxxxx", "xxxxx", "xxxxx");
```

#### 简单用法

修改参数后直接运行`MongoUtilSimple`下的main函数

#### 进阶解释

若mongo的表名是xxxx_1,xxxx_2,xxxx_3的类似命名,可通过`MongoUtilComplex`进行导出,`mongoCollerticonsPrefix `的值为xxxx,建表语句会删除后缀变成xxxx的表名,该类型的表的导出语句会额外储存在一个文件里.`outMongoCollerticonsPrefix `用于填写前缀相同的名字但是不希望被分类统计表,类似于xxxx_abcd

#####这都是写的什么鬼东西，麻烦这种东西设置私人项目
