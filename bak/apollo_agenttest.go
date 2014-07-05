package main

import (
	"apollo/amqp"
	"apollo/cfg"
	_ "apollo/mysql"
	"bytes"
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
	/*
		args := os.Args
		//get args
		publicIp = args[1]
		portS, _ = strconv.Atoi(args[2])
		portM, _ = strconv.Atoi(args[3])
		availPort = make(map[int]bool)
		for i := portS; i <= portM; i++ {
			availPort[i] = true
		}
	*/

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
	erre1 := mq_receive_channel_list.ExchangeDeclare(config["receive_exchange_name"], "direct", false, false, false, false, nil)
	if erre1 != nil {
		addErrorLog("receive channel list topic create exchange error: ", erre1)
		//os.Exit(0)
	}
	addLog("receive channel list topic create exchange OK!", config["receive_exchange_name"])

	_, errq := mq_receive_channel_list.QueueDeclare("apollotest", true, false, false, false, nil)
	if errq != nil {
		addErrorLog("receive queue act topic declare error :", errq)
		//  os.Exit(0)
	}
	errb := mq_receive_channel_list.QueueBind("apollotest", config["agent_queue_name"], config["receive_exchange_name"], false, nil)
	if errb != nil {
		addErrorLog("receive queue act topic bind error :", errb)
		//os.Exit(0)
	}
	addLog("queuebind : ", "apollotest", config["agent_queue_name"], config["receive_exchange_name"])

	//send channel
	/*
	   	var errc2 error
	   	mq_send_channel, errc2 = conn.Channel()
	   	if errc2 != nil {
	   		addErrorLog("send channel open error: ", errc2)
	   		os.Exit(0)
	   	}
	   	erre2 := mq_send_channel.ExchangeDeclare("linshi", "direct", true, false, false, false, nil)
	           if erre1 != nil {
	                   addErrorLog("send channel create exchange error: ", erre2)
	                   os.Exit(0)
	           }
	   str := `{"cmd":"common_reply","context":{"result":"0","taskid":"107"},"logid":"6129484611666145821"}`
	    msgSend := amqp.Publishing{
	                   DeliveryMode: amqp.Persistent,
	                   Timestamp:    time.Now(),
	                   ContentType:  "text/plain",
	                   //Body:         []byte(`{"cmd":"common_reply","context":{"result":"0","taskid":"107"},"logid":"6129484611666145821"}`),
	                   Body:         []byte(str),
	           }
	           err = mq_send_channel.Publish("linshi", "liuyanglongbaetest", false, false, msgSend)
	           if err != nil {
	                   addErrorLog("basic.publish send Error :", err)
	           }

	           addLog("send msg to mq OK!", "linshi")

	   	os.Exit(0)
	*/
}

//从MQ中获取消息
func main() {
	listTopic, errl := mq_receive_channel_list.Consume("apollotest", "", false, false, false, false, nil)
	if errl != nil {
		addErrorLog("basic.list topic : ", errl)
		// os.Exit(0)
	}
	for d := range listTopic {
		fmt.Println("get from service is :", string(d.Body))
		msgAction(d.Body, d.ReplyTo)
		d.Ack(false)
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
	sentQueueMsg(ok, agentMsg, replyto)
}

func getQueueMsg(data []byte) map[string]string {

	msgstr := data
	addLog("getdata is :", string(msgstr))
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

	return retMsg
}

//发送消息
func sentQueueMsg(ok bool, msg interface{}, reply string) {
	fmt.Println("reply is:", reply)
	jsonStr, _ := json.Marshal(msg)
	addLog(string(jsonStr), msg)

	msgSend := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         jsonStr,
	}
	conn, errmq := amqp.Dial(config["amqp_uri"])
	if errmq != nil {
		addErrorLog("connection.open Error:", errmq)
		os.Exit(0)
	}

	mq_send_channel, errc2 := conn.Channel()
	if errc2 != nil {
		addErrorLog("send channel open error: ", errc2)
		os.Exit(0)
	}
	erre2 := mq_send_channel.ExchangeDeclare("linshi", "direct", true, false, false, false, nil)
	if erre2 != nil {
		addErrorLog("send channel create exchange error: ", erre2)
		os.Exit(0)
	}
	addLog("send msg to mq OK!", "linshi")

	err := mq_send_channel.Publish("linshi", reply, false, false, msgSend)
	if err != nil {
		addErrorLog("basic.publish send Error :", err)
	}

	addLog("send msg to mq OK!", string(jsonStr), "linshi", reply)

}

//当actname = create时触发的动作，创建规则
func createProxyRule(qdata map[string]string) (map[string]string, bool) {
	resmsg := make(map[string]string)
	return resmsg, true
}

//actname = delete时触发的动作，删除规则
func deleteProxyRule(qdata map[string]string) (map[string]string, bool) {
	resmsg := make(map[string]string)
	return resmsg, true
}

//当actname = reset时触发的动作，清空server上所有的规则，然后重置
func resetProxyRule(qdata map[string]string) (map[string]string, bool) {
	resmsg := make(map[string]string)
	return resmsg, true
}

func getUsedPort() []int {
	var retPort []int
	return retPort
}

func addRule(uiIp string, pport int) bool {
	return true
}

func deleteRule(uiIp string, pport int) bool {
	return true
}

func resetRule() {
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
