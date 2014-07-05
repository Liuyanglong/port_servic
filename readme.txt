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
	10、bak/：基本是一些第一版中使用agent方案的代码文件
	11、bak/agent_config.cfg：测试agent使用的配置文件,格式为“key=value”
	12、bak/apollo_service.go: apollo服务进程
	13、bak/apollo_agenttest.go：测试agent
	14、apollo_test.go：测试case
	
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
	test get 1、portm-ports-num 有溢出bug，应该为 portm-ports+1-num
	test get 2、bgw api错误码细分，根据错误码做出对应策略；
	test get 3、golang mail，增加邮件报警
	test get 4、当资源不够10%时，报警

URL Case:
	1、增加rs server:
		curl -i -X POST 'http://host:port/apolloAdmin/rsServerMsg' -d '["hostname","hostname"]'
	2、删除rs server:
		curl -i -X DELETE 'http://host:port/apolloAdmin/rsServerMsg' -d '["hostname","hostname"]'
	3、申请service: curl -i -X GET 'localhost:9090/startService?platformId=1234'  
	4、分配app:curl -i -X POST http://localhost:9090/apolloService -d'{"appid":"appid3r9qwoeb9f","port":"16666","proto":"tcp","publicIp":"220.181.57.153","publicPort":"30002"}'
	5、模拟调度发送请求：	
	curl -i -X POST http://localhost:9090/apolloScheduler -d '{"appid":"yanglong_test1234","logId":12121212,"ports":[{"internalport":16666,"proto":"tcp","ipport":["10.209.78.11:30001","10.209.78.11:30002"]}]}'
	6、recovery恢复api:
		curl -i -X POST 'http://localhost:9090/apolloAdmin/recovery' -d '{"appid":"sssss","vip":"111.206.45.12","vport":"30000"}'

