# Mycat 优化改进
1. 修复SQL中包含OR条件时，路由问题将会导致SQL被所有节点执行
2. 可以支持SQL中包含 > (大于/大于等于) 和 < （小于/小于等于） 操作符,避免SQL路由到所有节点
3. 可以支持SQL中包含 != (不等于) 操作符，避免SQL路由到所有节点
4. 简单重构优化代码减少对象创建

# Mycat使用

## 运行：
### linux：
   ./mycat start 启动

   ./mycat stop 停止

   ./mycat console 前台运行

   ./mycat install 添加到系统自动启动（暂未实现）

   ./mycat remove 取消随系统自动启动（暂未实现）

   ./mycat restart 重启服务

   ./mycat pause 暂停

   ./mycat status 查看启动状态

### win：
直接运行startup_nowrap.bat，如果出现闪退，在cmd 命令行运行，查看出错原因。 

## 内存配置：
启动前，一般需要修改JVM配置参数，打开conf/wrapper.conf文件，如下行的内容为2G和2048，可根据本机配置情况修改为512M或其它值。
以下配置跟jvm参数完全一致，可以根据自己的jvm参数调整。

Java Additional Parameters

wrapper.java.additional.1=

wrapper.java.additional.1=-DMYCAT_HOME=.

wrapper.java.additional.2=-server

wrapper.java.additional.3=-XX:MaxPermSize=64M

wrapper.java.additional.4=-XX:+AggressiveOpts

wrapper.java.additional.5=-XX:MaxDirectMemorySize=100m

wrapper.java.additional.6=-Dcom.sun.management.jmxremote

wrapper.java.additional.7=-Dcom.sun.management.jmxremote.port=1984

wrapper.java.additional.8=-Dcom.sun.management.jmxremote.authenticate=false

wrapper.java.additional.9=-Dcom.sun.management.jmxremote.ssl=false

wrapper.java.additional.10=-Xmx100m

wrapper.java.additional.11=-Xms100m

wrapper.java.additional.12=-XX:+UseParNewGC

wrapper.java.additional.13=-XX:+UseConcMarkSweepGC

wrapper.java.additional.14=-XX:+UseCMSCompactAtFullCollection

wrapper.java.additional.15=-XX:CMSFullGCsBeforeCompaction=0

wrapper.java.additional.16=-XX:CMSInitiatingOccupancyFraction=70


以下配置作废：

wrapper.java.initmemory=3

wrapper.java.maxmemory=64

### Mycat连接测试：
测试mycat与测试mysql完全一致，mysql怎么连接，mycat就怎么连接。

推荐先采用命令行测试：

mysql -uroot -proot -P8066 -h127.0.0.1

如果采用工具连接，1.4,1.3目前部分工具无法连接，会提示database not selected，建议采用高版本，navicat测试。1.5已经修复了部分工具连接。


# Mycat配置入门

## 配置：
--bin  启动目录

--conf 配置文件存放配置文件：

      --server.xml：是Mycat服务器参数调整和用户授权的配置文件。

      --schema.xml：是逻辑库定义和表以及分片定义的配置文件。

      --rule.xml：  是分片规则的配置文件，分片规则的具体一些参数信息单独存放为文件，也在这个目录下，配置文件修改需要重启MyCAT。

      --log4j.xml： 日志存放在logs/log中，每天一个文件，日志的配置是在conf/log4j.xml中，根据自己的需要可以调整输出级别为debug                           debug级别下，会输出更多的信息，方便排查问题。

      --autopartition-long.txt,partition-hash-int.txt,sequence_conf.properties， sequence_db_conf.properties 分片相关的id分片规则配置文件

      --lib	    MyCAT自身的jar包或依赖的jar包的存放目录。

      --logs        MyCAT日志的存放目录。日志存放在logs/log中，每天一个文件

下面图片描述了Mycat最重要的3大配置文件：
<p>
	<img src="http://songwie.com/attached/image/20160205/20160205164558_154.png" alt="">
</p>

## 逻辑库配置：
### 配置server.xml
添加两个mycat逻辑库：user,pay:
system 参数是所有的mycat参数配置，比如添加解析器：defaultSqlParser，其他类推
user 是用户参数。

	<system>

		<property name="defaultSqlParser">druidparser</property>

	</system>

	<user name="mycat">

		<property name="password">mycat</property>

		<property name="schemas">user,pay</property>

	</user>

### 编辑schema.xml
修改dataHost和schema对应的连接信息，user,pay 垂直切分后的配置如下所示：

schema 是实际逻辑库的配置，user，pay分别对应两个逻辑库，多个schema代表多个逻辑库。

dataNode是逻辑库对应的分片，如果配置多个分片只需要多个dataNode即可。

dataHost是实际的物理库配置地址，可以配置多主主从等其他配置，多个dataHost代表分片对应的物理库地址，下面的writeHost、readHost代表该分片是否配置多写，主从，读写分离等高级特性。

以下例子配置了两个writeHost为主从。

    <schema name="user" checkSQLschema="false" sqlMaxLimit="100" dataNode="user" />
    <schema name="pay"  checkSQLschema="false" sqlMaxLimit="100" dataNode="pay" >
       <table name="order" dataNode="pay1,pay2" rule="rule1"/>
    </schema>

    <dataNode name="user" dataHost="host" database="user" />
    <dataNode name="pay1" dataHost="host" database="pay1" />
    <dataNode name="pay2" dataHost="host" database="pay2" />

    <dataHost name="host" maxCon="1000" minCon="10" balance="0"
       writeType="0" dbType="mysql" dbDriver="native">
       <heartbeat>select 1</heartbeat>
       <!-- can have multi write hosts -->
       <writeHost host="hostM1" url="192.168.0.2:3306" user="root" password="root" />
       <writeHost host="hostM2" url="192.168.0.3:3306" user="root" password="root" />
    </dataHost>
    
    

# Mycat逻辑库、系统参数配置

## 配置Mycat环境参数
    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <!DOCTYPE mycat:server SYSTEM "server.dtd">
    <mycat:server xmlns:mycat="http://org.opencloudb/">
       <system>
    	  <property name="defaultSqlParser">druidparser</property>
        </system> 
     </mycat:server>

如例子中配置的所有的Mycat参数变量都是配置在server.xml 文件中，system标签下配置所有的参数，如果需要配置某个变量添加相应的配置即可，例如添加启动端口8066，默认为8066：

       <property name="serverPort">8066</property>

其他所有变量类似。

## 配置Mycat逻辑库与用户

    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <!DOCTYPE mycat:server SYSTEM "server.dtd">
    <mycat:server xmlns:mycat="http://org.opencloudb/">
	<user name="mycat">
		<property name="password">mycat</property>
		<property name="schemas">TESTDB</property>
	</user>
     </mycat:server>


如例子中配置的所有的Mycat连接的用户与逻辑库映射都是配置在server.xml 文件中，user标签下配置所有的参数，例如例子中配置了一个mycat用户供应用连接到mycat，同时mycat 在schema.xml中配置后了一个逻辑库TESTDB，配置好逻辑库与用户的映射关系。


# 逻辑库、表分片配置

## 配置逻辑库（schema）

Mycat作为一个中间件，实现mysql协议那么对前端应用连接来说就是一个数据库，也就有数据库的配置，mycat的数据库配置是在schema.xml中配置，配置好后映射到server.xml里面的用户就可以了。

	<?xml version="1.0" encoding="UTF-8"?>
	<!DOCTYPE mycat:schema SYSTEM "schema.dtd">
	
	<mycat:schema  xmlns:mycat="http://org.opencloudb/">
	  <schema name="TESTDB" checkSQLschema="true" sqlMaxLimit="100" dataNode="dn1">
	      <table name="t_user" dataNode="dn1,dn2" rule="sharding-by-mod2"/>
	      <table name="ht_jy_login_log" primaryKey="ID" dataNode="dn1,dn2" rule="sharding-by-date_jylog"/>
	  </schema>
	  <dataNode name="dn1" dataHost="localhost1" database="mycat_node1"/>
	  <dataNode name="dn2" dataHost="localhost1" database="mycat_node2"/>
	  
	  <dataHost name="localhost1" writeType="0" switchType="1" slaveThreshold="100" balance="1" dbType="mysql" maxCon="10" minCon="1" dbDriver="native">
	    <heartbeat>show status like 'wsrep%'</heartbeat>
	    <writeHost host="hostM1" url="127.0.0.1:3306" user="root" password="root" >
	    </writeHost>  
	  </dataHost>
	</mycat:schema >

上面例子配置了一个逻辑库TESTDB，同时配置了t_user，ht_jy_login_log两个分片表。

### 逻辑表配置
	      <table name="t_user" dataNode="dn1,dn2" rule="sharding-by-mod2"/>

table 标签 是逻辑表的配置 其中 

name代表表名，

dataNode代表表对应的分片，

Mycat默认采用分库方式，也就是一个表映射到不同的库上，

rule代表表要采用的数据切分方式，名称对应到rule.xml中的对应配置，如果要分片必须配置。


## 配置分片（dataNode）

	  <dataNode name="dn1" dataHost="localhost1" database="mycat_node1"/>
	  <dataNode name="dn2" dataHost="localhost1" database="mycat_node2"/>

表切分后需要配置映射到哪几个数据库中，Mycat的分片实际上就是库的别名，例如上面例子配置了两个分片dn1，dn2 分别对应到物理机映射dataHost
localhost1 的两个库上。

## 配置物理库分片映射（dataHost）

	  <dataHost name="localhost1" writeType="0" switchType="1" slaveThreshold="100" balance="1" dbType="mysql" maxCon="10" minCon="1" dbDriver="native">
	    <heartbeat>show status like 'wsrep%'</heartbeat>
	    <writeHost host="hostM1" url="127.0.0.1:3306" user="root" password="root" >
	    </writeHost>  
	  </dataHost>

Mycat作为数据库代理需要逻辑库，逻辑用户，表切分后需要配置分片，分片也就需要映射到真实的物理主机上，至于是映射到一台还是一台的多个实例上，Mycat并不关心，只需要配置好映射即可，例如例子中：

配置了一个名为localhost1的物理主机（dataHost）映射。

heartbeat 标签代表Mycat需要对物理库心跳检测的语句，正常情况下生产案例可能配置主从，或者多写 或者单库，无论哪种情况Mycat都需要维持到数据库的数据源连接，因此需要定时检查后端连接可以性，心跳语句就是来作为心跳检测。

writeHost 此标签代表 一个逻辑主机（dataHost）对应的后端的物理主机映射，例如例子中写库hostM1 映射到127.0.0.1:3306。如果后端需要做读写分离或者多写 或者主从则通过配置 多个writeHost 或者readHost即可。

dataHost 标签中的 writeType balance 等标签则是不同的策略，具体参考指南。

# Mycat 表切分规则配置

## 表切分规则

	<?xml version="1.0" encoding="UTF-8"?>
	<!DOCTYPE mycat:rule SYSTEM "rule.dtd">
	
	<mycat:rule  xmlns:mycat="http://org.opencloudb/">
	  <tableRule name="sharding-by-hour">
	    <rule>
	      <columns>createTime</columns>
	      <algorithm>sharding-by-hour</algorithm>
	    </rule>
	  </tableRule>
	  
	  <function name="sharding-by-hour" class="org.opencloudb.route.function.LatestMonthPartion">
	    <property name="splitOneDay">24</property>
	  </function>
	   
	</mycat:rule >

数据切分中作为表切分规则中最重要的配置，表的切分方式决定了数据切分后的性能好坏，因此也是最重要的配置。

如上面例子配置了一个切分规则，名为sharding-by-hour 对应的切分方式（function ）是按日期切分，该配置中：

### tableRule

name 为schema.xml 中table 标签中对应的 rule="sharding-by-hour" ,也就是配置表的分片规则，

columns 是表的切分字段： createTime 创建日期。

algorithm 是规则对应的切分规则：映射到function 的name。


### function 

function 配置是分片规则的配置。

name 为切分规则的名称，名字人员取，但是需要与tableRule 中匹配。

class 是切分规则对应的切分类，写死，需要哪种规则则配置哪种，例如本例子是按小时分片：org.opencloudb.route.function.LatestMonthPartion

property 标签是切分规则对应的不同属性，不同的切分规则配置不同。


