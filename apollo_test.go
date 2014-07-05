package main

import (
	"apollo/amqp"
	"apollo/cfg"
	_ "apollo/mysql"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

var config map[string]string       //config
var agent_config map[string]string //config
var mq_receive_channel_list, mq_send_channel *amqp.Channel

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get config
	config = make(map[string]string)
	err := cfg.Load("service_config.cfg", config)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(0)
	}

	agent_config = make(map[string]string)
	err = cfg.Load("agent_config.cfg", agent_config)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(0)
	}

	//open mq channel
	conn, errmq := amqp.Dial(agent_config["amqp_uri"])
	if errmq != nil {
		os.Exit(0)
	}

	var errc1 error
	mq_receive_channel_list, errc1 = conn.Channel()
	if errc1 != nil {
		os.Exit(0)
	}
	erre1 := mq_receive_channel_list.ExchangeDeclare(agent_config["receive_exchange_name"], "direct", false, false, false, false, nil)
	if erre1 != nil {
		fmt.Println("error 1")
	}

	_, errq := mq_receive_channel_list.QueueDeclare("apollotest", true, false, false, false, nil)
	if errq != nil {
		fmt.Println("error 2")
	}
	errb := mq_receive_channel_list.QueueBind("apollotest", agent_config["agent_queue_name"], agent_config["receive_exchange_name"], false, nil)
	if errb != nil {
		fmt.Println("error 3")
	}

}

func Test_ok(t *testing.T) {
	fmt.Println("config:::::::", config)
	t.Log("第一个测试通过了")
}

//测试 get curl -i -X GET 'http://localhost:9090/startService
func Test_startServiceCase(t *testing.T) {
	conn := initDbConn()
	defer conn.Close()

	startTransaction(conn)
	defer rollback(conn)

	resp1 := requestURL("POST", "http://localhost:9090/startService", "")
	if resp1["result"] == "0" && resp1["return_status"] == "400" {
		t.Log("startService get 错误请求的正确响应case 1 通过")
	} else {
		t.Error("startService get 错误请求的正确响应case 1 Error!", resp1)
	}

	resp2 := requestURL("GET", "http://localhost:9090/startService", "")
	if resp2["result"] == "1" && resp2["return_status"] == "200" {
		t.Log("startService get 正确请求的正确响应case 2 通过")
	} else {
		t.Error("startService get 正确请求的正确响应case 2 Error!", resp2)
	}
}

//测试 POST http://[host]/apolloService
func Test_test1(t *testing.T) {
	conn := initDbConn()
	defer conn.Close()

	startTransaction(conn)
	defer rollback(conn)

	resp1 := requestURL("POST", "http://localhost:9090/apolloService", `{"appid":"616","proto":"udp"}`)
	if resp1["result"] == "0" && resp1["return_status"] == "400" {
		t.Log("apolloService post 错误请求的正确响应case 1 通过")
	} else {
		t.Error("apolloService post 错误请求的正确响应case 1 Error!", resp1)
	}

	resp2 := requestURL("POST", "http://localhost:9090/apolloService", `{"proto":"udp"}`)
	if resp2["result"] == "0" && resp2["return_status"] == "400" {
		t.Log("apolloService post 错误请求的正确响应case 2 通过")
	} else {
		t.Error("apolloService post 错误请求的正确响应case 2 Error!", resp2)
	}

	resp3 := requestURL("POST", "http://localhost:9090/apolloService", `{"appid":"111","port":"9616"}`)
	if resp3["result"] == "0" && resp3["return_status"] == "400" {
		t.Log("apolloService post 错误请求的正确响应case 3 通过")
	} else {
		t.Error("apolloService post 错误请求的正确响应case 3 Error!", resp3)
	}

	resp4 := requestURL("POST", "http://localhost:9090/apolloService", `{"appid":"616","port":"9666","proto":"udpdsfsd"}`)
	if resp4["result"] == "0" && resp4["return_status"] == "400" {
		t.Log("apolloService post 错误请求的正确响应case 4 通过")
	} else {
		t.Error("apolloService post 错误请求的正确响应case 4 Error!", resp4)
	}

	//pre act for case 6 & case 6
	pubres := requestURL("GET", "http://localhost:9090/startService", "")
	resp5 := requestURL("POST", "http://localhost:9090/apolloService", `{"appid":"616","port":"9666","proto":"udp","publicIp":"`+pubres["publicIp"]+`","publicPort":"`+pubres["publicPort"]+`"}`)
	if resp5["result"] == "1" && resp5["return_status"] == "200" {
		t.Log("apolloService post 正确请求的正确响应case 5 通过")
	} else {
		t.Error("apolloService post 正确请求的正确响应case 5 Error!", resp5, pubres)
	}

	resp6 := requestURL("POST", "http://localhost:9090/apolloService", `{"appid":"616","port":"9666","proto":"udp","publicIp":"`+pubres["publicIp"]+`","publicPort":"`+pubres["publicPort"]+`"}`)
	if resp6["result"] == "0" && resp6["return_status"] == "403" {
		t.Log("apolloService post 重复创建测试 case 6 通过")
	} else {
		t.Error("apolloService post 重复创建测试 case 6 Error!", resp6)
	}
	t.Log("test 1 ok")
}

//测试 DELETE http://[host]/apolloService
func Test_test2(t *testing.T) {

	conn := initDbConn()
	defer conn.Close()

	startTransaction(conn)
	defer rollback(conn)

	pubres := requestURL("GET", "http://localhost:9090/startService", "")
	resp1 := requestURL("POST", "http://localhost:9090/apolloService", `{"appid":"616","port":"9666","proto":"udp","publicIp":"`+pubres["publicIp"]+`","publicPort":"`+pubres["publicPort"]+`"}`)
	if resp1["result"] == "1" && resp1["return_status"] == "200" {
	} else {
		t.Error("apolloService delete 准备条件case 1 Error!", resp1)
		//os.Exit(0)
	}

	resp2 := requestURL("DELETE", "http://localhost:9090/apolloService", `{"appid":"616"}`)
	if resp2["result"] == "0" && resp2["return_status"] == "400" {
		t.Log("apolloService delete 错误请求正确响应 case 1 通过")
	} else {
		t.Error("apolloService delete 错误请求正确响应 case 1 Error!", resp2)
	}

	resp3 := requestURL("DELETE", "http://localhost:9090/apolloService", `{"appid":"616","port":"9666"}`)
	if resp3["result"] == "1" && resp3["return_status"] == "200" {
		t.Log("apolloService delete 正常请求响应 case 2 通过")
	} else {
		t.Error("apolloService delete 正常请求响应 case 2 Error!", resp3)
	}

}

//测试 POST http://[host]/apolloScheduler
func Test_test3(t *testing.T) {
	conn := initDbConn()
	defer conn.Close()

	startTransaction(conn)
	defer rollback(conn)

	resp1 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"appid":"111","internalPort":"9616","proto":"tcp", "ips":"111.111.111.111:773"}`)
	if resp1["result"] == "0" && resp1["return_status"] == "400" {
		t.Log("apolloScheduler post 错误请求的正确响应case 1 通过")
	} else {
		t.Error("apolloScheduler post 错误请求的正确响应case 1 Error!", resp1)
	}

	resp2 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"appid":"111","internalPort":"9616","proto":"tcp","logid":"777777"}`)
	if resp2["result"] == "0" && resp2["return_status"] == "400" {
		t.Log("apolloScheduler post 错误请求的正确响应case 2 通过")
	} else {
		t.Error("apolloScheduler post 错误请求的正确响应case 2 Error!", resp2)
	}

	resp3 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"appid":"111","internalPort":"9616", "ips":"111.111.111.111:773","logid":"777777"}`)
	if resp3["result"] == "0" && resp3["return_status"] == "400" {
		t.Log("apolloScheduler post 错误请求的正确响应case 3 通过")
	} else {
		t.Error("apolloScheduler post 错误请求的正确响应case 3 Error!", resp3)
	}

	resp4 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"appid":"111","proto":"tcp", "ips":"111.111.111.111:773","logid":"777777"}`)
	if resp4["result"] == "0" && resp4["return_status"] == "400" {
		t.Log("apolloScheduler post 错误请求的正确响应case 4 通过")
	} else {
		t.Error("apolloScheduler post 错误请求的正确响应case 4 Error!", resp4)
	}

	resp5 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"internalPort":"9616","proto":"tcp", "ips":"111.111.111.111:773","logid":"777777"}`)
	if resp5["result"] == "0" && resp5["return_status"] == "400" {
		t.Log("apolloScheduler post 错误请求的正确响应case 5 通过")
	} else {
		t.Error("apolloScheduler post 错误请求的正确响应case 5 Error!", resp5)
	}

	resp6 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"appid":"111","internalPort":"9616","proto":"tcp", "ips":"111.111.111.111:773","logid":"777777"}`)
	if resp6["result"] == "0" && resp6["return_status"] == "500" {
		t.Log("apolloScheduler post 正常请求内部错误响应case 6 通过")
	} else {
		t.Error("apolloScheduler post 正确请求内部错误响应case 6 Error!", resp6)
	}

	//pre act for case 7
	sql1 := "INSERT INTO appidProxyPort ( `appid`, `publicIp`, `proxyPort`, `internalPort`) VALUES ( '111', '192.168.110.223', '10002', '9616')"
	conn.Exec(sql1)

	resp7 := requestURL("POST", "http://localhost:9090/apolloScheduler", `{"appid":"111","internalPort":"9616","proto":"tcp", "ips":"111.111.111.111:773","logid":"777777"}`)
	if resp7["result"] == "1" && resp7["return_status"] == "200" {
		t.Log("apolloScheduler post 正常请求内部错误响应case 7 通过")
	} else {
		t.Error("apolloScheduler post 正确请求内部错误响应case 7 Error!", resp7)
	}
	//	receiveMQData()
	//	fmt.Println( "receiveMqData")
}

//测试 delete http://[host]/apolloScheduler
func Test_test4(t *testing.T) {
	conn := initDbConn()
	defer conn.Close()

	startTransaction(conn)
	defer rollback(conn)

	resp1 := requestURL("DELETE", "http://localhost:9090/apolloScheduler", `{"appid":"111", "ips":"111.111.111.111:773"}`)
	if resp1["result"] == "0" && resp1["return_status"] == "400" {
		t.Log("apolloScheduler delete 错误请求的正确响应case 1 通过")
	} else {
		t.Error("apolloScheduler delete 错误请求的正确响应case 1 Error!", resp1)
	}

	resp2 := requestURL("DELETE", "http://localhost:9090/apolloScheduler", `{"appid":"111","logid":"777777"}`)
	if resp2["result"] == "0" && resp2["return_status"] == "400" {
		t.Log("apolloScheduler delete 错误请求的正确响应case 2 通过")
	} else {
		t.Error("apolloScheduler delete 错误请求的正确响应case 2 Error!", resp2)
	}

	resp3 := requestURL("DELETE", "http://localhost:9090/apolloScheduler", `{"appid":"111", "ips":"111.111.111.111:773","logid":"777777"}`)
	if resp3["result"] == "1" && resp3["return_status"] == "200" {
		t.Log("apolloScheduler delete 正常请求响应case 3 通过")
	} else {
		t.Error("apolloScheduler delete 正确请响应case 3 Error!", resp3)
	}
}

func initDbConn() *sql.DB {
	dbHost := config["db_connect"]
	dbconn, err := sql.Open("mysql", dbHost)
	if err != nil {
		os.Exit(0)
	}
	return dbconn
}

func requestURL(method, url, questBody string) map[string]string {
	var httpReq *http.Request
	client := &http.Client{}
	httpReq, _ = http.NewRequest(method, url, strings.NewReader(questBody))
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpResp, _ := client.Do(httpReq)
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	result := make(map[string]string)
	json.Unmarshal(bodys, &result)
	result["return_status"] = strconv.Itoa(httpResp.StatusCode)
	return result
}

func startTransaction(conn *sql.DB) {
	//conn.Exec("START TRANSACTION")

	sqld1 := "delete from appidProxyPort"
	sqld2 := "delete from port_proxy_rule"
	//sqld3 := "delete from proxyServer"
	//sqld4 := "delete from taskTicket"

	fmt.Println(conn.Exec(sqld1))
	fmt.Println(conn.Exec(sqld2))
	//conn.Exec(sqld3)
	//conn.Exec(sqld4)
}

func rollback(conn *sql.DB) {
	//conn.Exec("ROLLBACK")
}

func receiveMQData() {
	listTopic, errl := mq_receive_channel_list.Consume("apollotest", "", false, false, false, false, nil)
	if errl != nil {
	}
	for d := range listTopic {
		fmt.Println("get from service is :", string(d.Body))
		msgAction(d.Body, d.ReplyTo)
		d.Ack(false)
		//	break
	}
}

func msgAction(data []byte, replyto string) {
	//从mq中获取action
	msgData := getQueueMsg(data)
	agentMsg := make(map[string]interface{})
	context := make(map[string]string)
	var ok bool
	agentMsg["logid"] = msgData["logid"]
	agentMsg["cmd"] = "common_reply"
	context["taskid"] = msgData["taskid"]
	context["result"] = "0"
	agentMsg["context"] = context
	fmt.Println("sent data is :", agentMsg)
	sentQueueMsg(ok, agentMsg, replyto)
}

func getQueueMsg(data []byte) map[string]string {

	msgstr := data
	var retMsg map[string]string
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal(msgstr, &objmap)
	if err != nil {
		return retMsg
	}
	json.Unmarshal(*objmap["context"], &retMsg)

	var logid, cmd string
	json.Unmarshal(*objmap["logid"], &logid)
	json.Unmarshal(*objmap["cmd"], &cmd)
	retMsg["logid"] = logid
	retMsg["cmd"] = cmd

	return retMsg
}

//发送消息
func sentQueueMsg(ok bool, msg interface{}, reply string) {
	fmt.Println("reply is:", reply)
	jsonStr, _ := json.Marshal(msg)
	fmt.Println("send msg is:", string(jsonStr))

	msgSend := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         jsonStr,
	}
	conn, errmq := amqp.Dial(agent_config["amqp_uri"])
	fmt.Println("amqp_url: ", agent_config["amqp_uri"])
	if errmq != nil {
		fmt.Println("error 1")
	}

	mq_send_channel, errc2 := conn.Channel()
	if errc2 != nil {
		fmt.Println("error 2")
	}
	/*
		erre2 := mq_send_channel.ExchangeDeclare("linshi0", "direct", false, false, false, false, nil)
		if erre2 != nil {
			fmt.Println("error 3",erre2)
		}
	*/

	err := mq_send_channel.Publish("", reply, false, false, msgSend)
	if err != nil {
		fmt.Println("error 4", err)
	}
}

/*
func initCase1( conn *sql.DB ) {
sqli := "INSERT INTO `apollo`.`proxyServer` ( `publicIp`,`portS`,`portM`, `proxyServerIp`, `rulesNumber`,`isAvailable`) VALUES (?,?,?,?,?);"
		  stmt, _ := dbconn.Prepare(sqli)
		  if result, err := stmt.Exec("10.10.10.10",10000,10030,"123.126.126.126",10,1); err == nil {
		  }   `
}
*/
