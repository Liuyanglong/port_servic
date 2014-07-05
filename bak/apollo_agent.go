package main

import (
	"apollo/amqp"
	"apollo/cfg"
	_ "apollo/mysql"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var publicIp string          //启动agent时对应的public ip
var portS int                //启动agent时对应的最小port
var portM int                //启动agent时对应的最大port
var availPort map[int]bool   //当前可用的端口池
var config map[string]string //
var receiveData map[string]string
var logNormal *log.Logger
var logError *log.Logger
var mq_receive_channel_list, mq_receive_channel_act, mq_send_channel *amqp.Channel

func init() {
	args := os.Args
	//get args
	publicIp = args[1]
	portS, _ = strconv.Atoi(args[2])
	portM, _ = strconv.Atoi(args[3])
	availPort = make(map[int]bool)
	for i := portS; i <= portM; i++ {
		availPort[i] = true
	}

	//get config
	config = make(map[string]string)
	err := cfg.Load("agent_config.cfg", config)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}

	//open log file
	logNFile, err1 := os.OpenFile(config["normal_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	logEFile, err2 := os.OpenFile(config["error_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err1 != nil || err2 != nil {
		fmt.Println("open log file Error!", err1, err2)
		os.Exit(0)
	}
	logNormal = log.New(logNFile, "\n", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	logError = log.New(logEFile, "\n", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	addLog("========================================\n", config, publicIp, portM, portS)

	//open mq channel
	conn, errmq := amqp.Dial(config["amqp_uri"])
	if errmq != nil {
		addErrorLog("connection.open Error:", errmq)
		os.Exit(0)
	}
	addLog("amqp connect OK!")

	var errc1 error
	mq_receive_channel_list, errc1 = conn.Channel()
	if errc1 != nil {
		addErrorLog("receive channel list topic open error: ", errc1)
		os.Exit(0)
	}
	addLog("create receive channel list topic OK!")
	erre1 := mq_receive_channel_list.ExchangeDeclare(config["receive_exchange_name"]+"_list", "topic", true, false, false, false, nil)
	if erre1 != nil {
		addErrorLog("receive channel list topic create exchange error: ", erre1)
		os.Exit(0)
	}
	addLog("receive channel list topic create exchange OK!")

	queueName := getQueueName()
	_, errq := mq_receive_channel_list.QueueDeclare(queueName, true, false, false, false, nil)
	if errq != nil {
		addErrorLog("receive queue list topic declare error :", errq)
		os.Exit(0)
	}
	addLog("receive queue list topic " + queueName + " create OK!")
	errb := mq_receive_channel_list.QueueBind(queueName, "apollo.#", config["receive_exchange_name"]+"_list", false, nil)
	if errb != nil {
		addErrorLog("receive queue list topic bind error :", errb)
		os.Exit(0)
	}
	addLog("receive queue list topic "+queueName+" bind OK!", config["receive_key_pre"])

	//open act topic
	mq_receive_channel_act, errc1 = conn.Channel()
	if errc1 != nil {
		addErrorLog("receive channel act topic open error: ", errc1)
		os.Exit(0)
	}
	addLog("create receive channel act topic OK!")
	erre1 = mq_receive_channel_act.ExchangeDeclare(config["receive_exchange_name"]+"_act", "topic", true, false, false, false, nil)
	if erre1 != nil {
		addErrorLog("receive channel act topic create exchange error: ", erre1)
		os.Exit(0)
	}
	addLog("receive channel act topic create exchange OK!")

	_, errq = mq_receive_channel_act.QueueDeclare(queueName, true, false, false, false, nil)
	if errq != nil {
		addErrorLog("receive queue act topic declare error :", errq)
		os.Exit(0)
	}
	addLog("receive queue act topic " + queueName + " create OK!")
	errb = mq_receive_channel_act.QueueBind(queueName, "apollo."+publicIp+"_"+strconv.Itoa(portS)+"_"+strconv.Itoa(portM)+".#", config["receive_exchange_name"]+"_act", false, nil)
	if errb != nil {
		addErrorLog("receive queue act topic bind error :", errb)
		os.Exit(0)
	}
	addLog("receive queue act topic "+queueName+" bind OK!", config["receive_key_pre"])

	//send channel
	var errc2 error
	mq_send_channel, errc2 = conn.Channel()
	if errc2 != nil {
		addErrorLog("send channel open error: ", errc2)
		os.Exit(0)
	}

	addLog("create send channel OK!")
	erre2 := mq_send_channel.ExchangeDeclare(config["send_exchange_name"], "direct", true, false, false, false, nil)
	if erre1 != nil {
		addErrorLog("send channel create exchange error: ", erre2)
		os.Exit(0)
	}
	addLog("send channel create exchange OK!")

	//获取当前proxy server上所有已经正在用的port
	usedPorts := getUsedPort()
	for _, value := range usedPorts {
		delete(availPort, value)
	}

}

//从MQ中获取消息
func main() {
	listTopic, errl := mq_receive_channel_list.Consume(getQueueName(), "", false, false, false, false, nil)
	if errl != nil {
		addErrorLog("basic.list topic : ", errl)
		os.Exit(0)
	}
	actTopic, erra := mq_receive_channel_act.Consume(getQueueName(), "", false, false, false, false, nil)
	if erra != nil {
		addErrorLog("basic.act topic : ", erra)
		os.Exit(0)
	}
	go func() {
		for d := range listTopic {
			fmt.Println(string(d.Body))
			msgAction(d.Body)
			d.Ack(false)
		}
	}()
	func() {
		for d := range actTopic {
			fmt.Println(string(d.Body))
			msgAction(d.Body)
			d.Ack(false)
		}
	}()
}

func msgAction(data []byte) {
	//从mq中获取action
	msgData := getQueueMsg(data)
	receiveData = msgData
	action, actok := msgData["actnum"] //获取action number
	if actok != true {
		sentQueueMsg(false, "sent msg error: not have action!")
		return
	}
	var agentMsg map[string]string
	var ok bool
	switch action {
	case "1": //showProxyMsg
		agentMsg, ok = showProxyMsg()
	case "2": //CreateProxyRule
		agentMsg, ok = createProxyRule(msgData)
	case "3": //DeleteProxyRule
		agentMsg, ok = deleteProxyRule(msgData)
	case "4": //resetProxyRule
		agentMsg, ok = resetProxyRule(msgData)
	default:
		sentQueueMsg(false, "we don`t know this action!")
		return
	}
	sentQueueMsg(ok, agentMsg)
}

func getQueueMsg(data []byte) map[string]string {

	msgstr := data
	//	os.Exit(0)
	//msgstr := []byte(`{"logid":"123412341234","cmd":"show_proxy_load","context":{"taskid":"616616616","actname":"show","actnum":"1"}}`)
	var retMsg map[string]string
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal(msgstr, &objmap)
	if err != nil {
		addErrorLog("receive msg error:", err, string(msgstr))
		return retMsg
	}
	json.Unmarshal(*objmap["context"], &retMsg)

	var logid, cmd string
	json.Unmarshal(*objmap["logid"], &logid)
	json.Unmarshal(*objmap["cmd"], &cmd)
	retMsg["logid"] = logid
	retMsg["cmd"] = cmd
	//fmt.Println(retMsg)
	//os.Exit(0)

	addLog("receive msg is :", string(msgstr))
	/*
		retMsg["actnum"] = "1"
		retMsg["actname"] = "show"

		retMsg["actnum"] = "2"
		retMsg["actname"] = "create"
		retMsg["port"] = "10029"
		retMsg["uiIp"] = "192.168.110.225:10051"

		retMsg["actnum"] = "3"
		retMsg["actname"] = "delete"
		retMsg["port"] = "10029"

		retMsg["actnum"] = "4"
		retMsg["actname"] = "reset"
		retMsg["proxyIp"] = "10.81.64.110"
	*/
	return retMsg
}

//发送消息
func sentQueueMsg(ok bool, msg interface{}) {
	sentData := make(map[string]string) //bixu!
	switch msg.(type) {
	case string:
		sentData["msg"] = msg.(string)
	case map[string]string:
		sentData = msg.(map[string]string)
	}

	jsonStr, _ := json.Marshal(sentData)
	addLog(string(jsonStr), sentData)

	msgSend := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         jsonStr,
	}
	err := mq_send_channel.Publish(config["send_exchange_name"], config["send_key"], false, false, msgSend)
	if err != nil {
		addErrorLog("basic.publish send Error :", err)
	}
	addLog("send msg to mq OK!", string(jsonStr))

}

//actname = show 时触发的动作，获取当前server的资源信息
func showProxyMsg() (map[string]string, bool) {
	resmsg := make(map[string]string)
	resmsg["result"] = "1"
	ipaddr := shellCmd("hostname", "-i")
	resmsg["server_ip"] = ipaddr
	resmsg["pub_ip"] = publicIp
	resmsg["proxy_port_range"] = strconv.Itoa(portS) + "-" + strconv.Itoa(portM)
	load := shellCmd("/bin/sh", "-c", `netstat -tan|grep ESTABLISHED|wc -l`)
	resmsg["server_load"] = load
	for i, v := range availPort {
		if v == true {
			resmsg["avail_port"] = strconv.Itoa(i)
			break
		}
	}
	addLog("showProxyMsg:", resmsg)
	return resmsg, true
}

//当actname = create时触发的动作，创建规则
func createProxyRule(qdata map[string]string) (map[string]string, bool) {
	resmsg := make(map[string]string)
	resmsg["result"] = "1"
	port, err1 := qdata["port"]
	uiIp, err2 := qdata["uiIp"]
	if err1 != true || err2 != true {
		resmsg["result"] = "0"
		resmsg["error"] = "create proxy rule Error!"
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("createProxyRule:", resmsg)
		return resmsg, false
	}

	portInt, _ := strconv.Atoi(port)
	if portInt < portS || portInt > portM {
		resmsg["result"] = "0"
		resmsg["error"] = "porxy port Error!"
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("createProxyRule:", resmsg)
		return resmsg, false
	}

	//将rule加到db中
	addRes := addRule(uiIp, portInt)
	if addRes != true {
		resmsg["result"] = "0"
		resmsg["error"] = "add Rule failed:" + port + "--" + uiIp
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("createProxyRule:", resmsg)
		return resmsg, false
	}

	//将规则加到server
	//shellCmd("iptables", "-t", "nat", "-I", "PREROUTING", "-p", "tcp", "--dport", port, "-j", "DNAT", "--to", uiIp)
	//shellCmd("iptables", "-t", "nat", "-I", "POSTROUTING", "-p", "tcp", "--dport", port, "-j", "MASQUERADE")
	addLog("create OK", uiIp, port)

	//维护availPort可用端口池
	delete(availPort, portInt)
	return resmsg, true
}

//actname = delete时触发的动作，删除规则
func deleteProxyRule(qdata map[string]string) (map[string]string, bool) {
	resmsg := make(map[string]string)
	resmsg["result"] = "1"
	port, err1 := qdata["port"]
	if err1 != true {
		resmsg["result"] = "0"
		resmsg["error"] = "delete proxy rule Error!"
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("deleteProxyRule:", resmsg)
		return resmsg, false
	}
	portInt, _ := strconv.Atoi(port)
	if portInt < portS || portInt > portM {
		resmsg["result"] = "0"
		resmsg["error"] = "porxy port Error at delete action! "
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("deleteProxyRule:", resmsg)
		return resmsg, false
	}

	//从db中删除规则
	delRes := deleteRule(publicIp, portInt)
	if delRes != true {
		resmsg["result"] = "0"
		resmsg["error"] = "delete Rule failed:" + port
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("deleteProxyRule:", resmsg)
		return resmsg, false
	}

	//删除规则
	//shellCmd("iptables", "-t", "nat", "-D", "PREROUTING", "-p", "tcp", "--dport", port, "-j", "DNAT", "--to", uiIp)
	//shellCmd("iptables", "-t", "nat", "-D", "POSTROUTING", "-p", "tcp", "--dport", port, "-j", "MASQUERADE")
	addLog("delete OK", publicIp, port)

	//维护availPort可用port池
	availPort[portInt] = true
	return resmsg, true
}

//当actname = reset时触发的动作，清空server上所有的规则，然后重置
func resetProxyRule(qdata map[string]string) (map[string]string, bool) {
	resmsg := make(map[string]string)
	resmsg["result"] = "1"
	proxyIp, err1 := qdata["proxyIp"]
	if err1 != true {
		resmsg["result"] = "0"
		resmsg["error"] = "reset proxy rule Error!"
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("resetProxyRule:", resmsg)
		return resmsg, false
	}
	ipaddr := shellCmd("hostname", "-i")
	if proxyIp != ipaddr {
		resmsg["result"] = "0"
		resmsg["error"] = "proxy ip != this ip address!"
		qdataStr, _ := json.Marshal(qdata)
		resmsg["errorStruct"] = string(qdataStr)
		addErrorLog("resetProxyRule:", resmsg)
		return resmsg, false
	}

	//清空rule规则
	//shellCmd("iptables", "-t", "nat", "-F")
	addLog("clear iptables")

	resetRule()
	return resmsg, true
}

func initDbConn() *sql.DB {
	dbHost := config["db_connect"]
	//fmt.Println("db host is :", dbHost)
	dbconn, err := sql.Open("mysql", dbHost)
	if err != nil {
		addErrorLog("initDbConn:Db Error!", err)
		os.Exit(1)
	}
	return dbconn
}
func getUsedPort() []int {
	dbconn := initDbConn()
	defer dbconn.Close()
	var retPort []int
	sqls := "select proxyPort from port_proxy_rule where publicIp = '" + publicIp + "' and proxyPort >= '" + strconv.Itoa(portS) + "' and proxyPort <= '" + strconv.Itoa(portM) + "'"
	addLog("getUsedPort:", sqls)
	rows, err := dbconn.Query(sqls)
	if err != nil {
		addErrorLog("get Msg error:", err)
		return retPort
	}

	var port int
	for rows.Next() {
		if err := rows.Scan(&port); err == nil {
			retPort = append(retPort, port)
		}
	}

	return retPort
}

func addRule(uiIp string, pport int) bool {
	dbconn := initDbConn()
	defer dbconn.Close()
	sqls := "insert into port_proxy_rule(publicIp,proxyPort,uiIpPort) values (?,?,?)"
	addLog("addRule:", sqls, publicIp, pport, uiIp)
	stmt, _ := dbconn.Prepare(sqls)
	if result, err := stmt.Exec(publicIp, pport, uiIp); err == nil {
		if id, err := result.LastInsertId(); err == nil {
			addLog("addRule insert id : ", id)
			return true
		}
	}
	return false
}

func deleteRule(uiIp string, pport int) bool {
	dbconn := initDbConn()
	defer dbconn.Close()
	sqls := "delete from port_proxy_rule where publicIp=? and proxyPort=?"
	addLog("deleteRule:", sqls, publicIp, pport)
	stmt, _ := dbconn.Prepare(sqls)
	if result, err := stmt.Exec(publicIp, pport); err == nil {
		if num, err := result.RowsAffected(); err == nil {
			addLog("deleteRule delete result : ", num)
			return true
		}
	}
	return false
}

func resetRule() {
	dbconn := initDbConn()
	defer dbconn.Close()
	sqls := "select proxyPort,uiIpPort from port_proxy_rule where publicIp = '" + publicIp + "' and proxyPort >= '" + strconv.Itoa(portS) + "' and proxyPort <= '" + strconv.Itoa(portM) + "'"
	addLog("resetRule:", sqls)
	rows, err := dbconn.Query(sqls)
	if err != nil {
		addErrorLog("resetRule:get uiIpPort Msg error:", err)
		return
	}

	var port int
	var uiIp string
	for rows.Next() {
		if err := rows.Scan(&port, &uiIp); err == nil {
			//shellCmd("iptables", "-t", "nat", "-I", "PREROUTING", "-p", "tcp", "--dport", port, "-j", "DNAT", "--to", uiIp)
			//shellCmd("iptables", "-t", "nat", "-I", "POSTROUTING", "-p", "tcp", "--dport", port, "-j", "MASQUERADE")
			addLog("reset OK", uiIp, port)
		}
	}
}

func shellCmd(name string, arg ...string) string {
	cmd := exec.Command(name, arg...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		addErrorLog("cmd failed.", err, name, arg)
		return ""
	}
	return strings.Trim(out.String(), "\n")
}

func addLog(arg ...interface{}) {
	arg = append(arg, "[logid:"+receiveData["logid"]+"]", "[taskid:"+receiveData["taskid"]+"]")
	fmt.Println(arg...)
}

func addErrorLog(arg ...interface{}) {
	arg = append(arg, "[logid:"+receiveData["logid"]+"]", "[taskid:"+receiveData["taskid"]+"]")
	fmt.Println(arg...)
}

func getQueueName() string {
	return config["is_master"] + "_" + publicIp + "_" + strconv.Itoa(portS) + "_" + strconv.Itoa(portM)
}
