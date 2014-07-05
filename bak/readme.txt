源码文件说明（没有后缀的全是二进制文件）：
	1、amqp/：rabbitmq client；
	2、mysql/：mysql client;
	3、cfg/：读取配置文件类；
	4、test_example/: 测试子功能时的代码及可执行文件放在其中
	5、test_example/cfgtest.go：测试是否正常读取配置文件；
	6、test_example/mysqltest.go：测试是否正常连接到db，并正常读写；
	7、test_example/producer.go: 测试amqp的生产者
	8、test_example/consumer.go: 测试amqp的消费者
	9、service_config.cfg：apollo服务使用的配置文件,格式为“key=value”
	10、agent_config.cfg：测试agent使用的配置文件,格式为“key=value”
	11、apollo_service.go: apollo服务进程
	12、apollo_agenttest.go：测试agent
	13、apollo_test.go：测试case
	
手动编译安装：
	1、将源码checkout到$GOPATH/src下；
	2、cd apollo
	3、go build apollo_service.go
	4、得到可执行文件 apollo_service
	

测试case：
	1、test case中不正确请求的返回，正确请求的返回；
	2、各个api正确请求的逻辑处理；
	3、各个api重复动作的处理返回；
	4、不停的startService，直到端口分配完的逻辑处理；
	5、在4的基础上执行deleteService，然后再执行startService测试；
	6、在4的基础上，停掉service进程，然后重启在执行startService；