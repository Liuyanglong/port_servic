package main

import (
	//"apollo/amqp"
	. "apollo/apolloRabbitMq"
	"apollo/cfg"
	"apollo/mail"
	_ "apollo/mysql"
	"apollo/portcheck"
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/bitly/go-simplejson"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//调度调用apolloScheduler Api的参数模型
type schedulerObject struct {
	Appid string
	Logid int64
	Ports []PortType
}
type PortType struct {
	Internalport int
	Proto        string
	Ipport       []string
}

//bgw接口的返回数据模型
type RsInfo struct {
	Status  int
	Message []RsInfoObj
}
type RsInfoObj struct {
	Host string
	Id   string
	Ip   string
	Type string
}

var config map[string]string //config
var logNormal, logError, logHeart *log.Logger
var portAvail map[string]map[int]bool                                      //可用的public ip和port
var serverNotifyTime map[string]int                                        //第一版本中验活agent，现暂时无用
var mq_receive_channel *RabbitMQ                                           //mq handle，暂时无用
var incApi string = "/app/incPortService"                                  //调度api，增加
var decApi string = "/app/decPortService"                                  //调度api，减少
var vcode string = ""                                                      //同调度api交互需要的验证code
var rsInfoApi string = `/?r=interface/api&handler=getRsInfoByHosts&hosts=` //bgw api，查询server的rs信息
var rsActApi string = `/?r=interface/api&handler=addDelRs&department=MCO`  //bgw api，更改vhost:vport下对应的rs
var rsCheckRsApi string = `/?r=interface/api&handler=getRsInfoRsIpSort`    //bgw api, 查询rs 是否成功建立的api
var rsGetRsInfoApi string = `/?r=interface/api&handler=getRsInfo`
var rsChnRsWeight string = `/?r=interface/api&handler=chnRsWeight`

var defaultRsIp string = ""
var defaultRsPort string = ""
var mailHost string = ""     //send mail host
var mailSendFrom string = "" //send mail user from
var mailSendTo string = ""   //send mail user to
var debug int = 0
var test_error_code = 0
var disable_server_id = "286974" //this server id is no use
var rms_token = "ecbf51b0980c88f4@MCO"
var department = "MCO"
var productname = "YUN"

//before main, init function
func init() {
	//调用全部cpu资源
	runtime.GOMAXPROCS(runtime.NumCPU())
	config_file := flag.String("config", "/home/bae/portservice/service_config.cfg", "config file")
	flag.Parse()
	//get config，将配置文件加载入内存
	config = make(map[string]string)
	err := cfg.Load(*config_file, config)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(0)
	}

	debug, _ = strconv.Atoi(config["debug"])
	test_error_code, _ = strconv.Atoi(config["test_error_code"])
	mailHost = config["mail_host"]
	mailSendFrom = config["mail_from"]
	mailSendTo = config["mail_to"]
	incApi = config["sch_addr"] + incApi
	decApi = config["sch_addr"] + decApi
	vcode = config["sch_vcode"]
	defaultRsIp = config["defaultRsIp"]
	defaultRsPort = config["defaultRsPort"]
	rsActApi = config["bgw_api_addr"] + rsActApi
	rsInfoApi = config["bgw_api_addr"] + rsInfoApi
	rsCheckRsApi = config["bgw_api_addr"] + rsCheckRsApi
	//open log file
	logNFile, err1 := os.OpenFile(config["normal_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	logEFile, err2 := os.OpenFile(config["error_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	logHFile, err3 := os.OpenFile(config["heart_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664) //此版本中不存在心跳验活，因此此logfile暂时无用
	if err1 != nil || err2 != nil || err3 != nil {
		fmt.Println("open log file Error!", err1, err2, err3)
		os.Exit(0)
	}
	logNormal = log.New(logNFile, ">", log.Ldate|log.Ltime|log.Lmicroseconds)
	logError = log.New(logEFile, ">", log.Ldate|log.Ltime|log.Lmicroseconds)
	logHeart = log.New(logHFile, ">", log.Ldate|log.Ltime|log.Lmicroseconds)
	addLog(0, "========================================\n", config)

	//初始化可用publicip和port
	initAvailPort(0)
}

//初始化可用publicip和port
func initAvailPort(logId int64) {
	conn := initDbConn(0)
	defer conn.Close()
	portAvail = make(map[string]map[int]bool)
	sql1 := "SELECT distinct publicIp,portS,portM  FROM proxyServer"
	rows, err := conn.Query(sql1)
	if err != nil {
		addErrorLog(logId, sql1, " initAvailPort error:", err)
		os.Exit(0)
	}

	var pubip, ports, portm string
	var totalNum float32 //all ports number
	var availNum float32 //all avail ports number

	for rows.Next() {
		if err := rows.Scan(&pubip, &ports, &portm); err == nil {
			portAvail[pubip] = make(map[int]bool)
			ps, _ := strconv.Atoi(ports)
			pm, _ := strconv.Atoi(portm)
			for ; ps <= pm; ps++ {
				portAvail[pubip][ps] = true
				totalNum++
				availNum++
			}
		}
	}

	sql2 := "select distinct publicIP,proxyPort from appidProxyPort"
	rows, err = conn.Query(sql2)
	if err != nil {
		addErrorLog(logId, sql2, " initAvailPort error:", err)
		os.Exit(0)
	}
	for rows.Next() {
		if err := rows.Scan(&pubip, &ports); err == nil {
			ps, _ := strconv.Atoi(ports)
			delete(portAvail[pubip], ps)
			availNum--
		}
	}
	for vip, vports := range portAvail {
		addLog(logId, "vip :", vip, " has availble vport number: ", len(vports))
	}
	if debug == 1 {
		fmt.Println("initAvailPort:", logId, "totalNum:", totalNum, "availNum:", availNum, "bili:", availNum/totalNum, "is less then 10%:", availNum/totalNum <= 0.1)
	}
	if availNum/totalNum <= 0.1 {
		smerr := sentMail("Warning! Warning!! Warning!!! ", "all avail ports number is less than 10%!!", "avail number is :", availNum)
		if debug == 1 {
			fmt.Println("initAvailPort: send mail has error:", smerr)
		}
	}
	return
}

//初始化数据库
func initDbConn(logId int64) *sql.DB {
	dbHost := config["db_connect"]
	addLog(logId, "db host is :", dbHost)
	dbconn, err := sql.Open("mysql", dbHost)
	if err != nil {
		addErrorLog(logId, "initDbConn:Db Error!", err)
		os.Exit(0)
	}
	return dbconn
}

func main() {
	logId := randNum()
	handleErr := make(chan bool)
	go httpHandle(handleErr, logId)
	go portCheckAvail()
	go portStatisticsCheck()

	<-handleErr
	addErrorLog(logId, "Error Happended!")
}

func portCheckAvail() {
	sqls := "SELECT appid,publicIp,proxyPort,internalPort FROM appidProxyPort where avail = 1"
	for {
		var appid string
		var pubip string
		var pubport int
		var interport int
		var sendmsg string = ""
		var errportnum int = 0
		dbconn := initDbConn(0)
		//defer dbconn.Close()
		rows, errc := dbconn.Query(sqls)
		if errc != nil {
			fmt.Println("portCheckAvail.query sql error happened!", errc)
			addErrorLog(0, "[portCheckAvail] query sql error happened:", sqls, errc)
			return
		}
		errportnum = 0
		sendmsg = "[portCheckAvail]<br>"
		for rows.Next() {
			err := rows.Scan(&appid, &pubip, &pubport, &interport)
			if err != nil {
				addErrorLog(0, "[portCheckAvail]. row scan error happened :", sqls, err)
				continue
			}
			ip_port := pubip + ":" + strconv.Itoa(pubport)
			is_avail, msg := portcheck.CheckPortAvail(ip_port, debug)
			if is_avail == false && msg != nil {
				errportnum += 1
				sendmsg += ("port check:  <b>" + appid + ":" + strconv.Itoa(interport) + "    " + ip_port + "</b>  is not available!" + msg.Error() + "<br>")
			}
			if debug == 1 {
				fmt.Println("[portCheckAvail]", "port check:", ip_port, " check available!", msg, is_avail)
			}
		}
		if errportnum > 0 {
			sentMail("total number is:" + strconv.Itoa(errportnum) + "<br>" + sendmsg)
		}
		dbconn.Close()
		//checktime, _ := strconv.Atoi(config["PortCheckAvail_Time"])
		time.Sleep(time.Second * 180)
	}
}

func portStatisticsCheck() {
	sql1 := "select count(*) as c from appidProxyPort"
	sql2 := "select count(*) as c from appidProxyPort where appid!='0'"
	sql3 := "select count(*) as c from port_proxy_rule"
	sql4 := "select count(*) as c from port_proxy_rule where bgwact !=0"
	for {
		dbconn := initDbConn(0)
		var all_appid_num, used_app_num, all_rules, used_rules int
		//defer dbconn.Close()
		rows, err := dbconn.Query(sql1)
		if rows.Next() {
			rows.Scan(&all_appid_num)
			if debug == 1 {
				fmt.Println("portStatisticsCheck error 1:", err, all_appid_num)
			}
		}
		rows, err = dbconn.Query(sql2)
		if rows.Next() {
			rows.Scan(&used_app_num)
			if debug == 1 {
				fmt.Println("portStatisticsCheck error 2:", err, used_app_num)
			}
		}
		rows, _ = dbconn.Query(sql3)
		if rows.Next() {
			rows.Scan(&all_rules)
			if debug == 1 {
				fmt.Println("portStatisticsCheck error 3:", err, all_rules)
			}
		}
		rows, _ = dbconn.Query(sql4)
		if rows.Next() {
			rows.Scan(&used_rules)
			if debug == 1 {
				fmt.Println("portStatisticsCheck error 4:", err, used_rules)
			}
		}
		mailBody := "<br>" +
			" all appid number is: " + strconv.Itoa(all_appid_num) + "<br>" +
			" used appid number is: " + strconv.Itoa(used_app_num) + "<br>" +
			" all rules is: " + strconv.Itoa(all_rules) + "<br>" +
			" used rules is: " + strconv.Itoa(used_rules) + "<br>"
		sentStatisticsMail(mailBody)
		dbconn.Close()
		//checktime, _ := strconv.Atoi(config["Statistics_Check_Time"])
		time.Sleep(time.Second * 7200)
	}
}

func httpHandle(errH chan bool, logId int64) {
	port, err := config["public_api_port"]
	if err != true {
		addErrorLog(logId, err)
		errH <- false
	}
	http.HandleFunc("/startService", startService)   //开启服务，从availPort资源池中分配一个可用的publicip & port
	http.HandleFunc("/deleteService", deleteService) //关闭服务，将已经分配的publicip & port 回收
	http.HandleFunc("/serviceCheck", serviceCheck)
	http.HandleFunc("/apolloService", apolloService)         //将appid对应的应用针对某个publicip&port下的操作
	http.HandleFunc("/apolloScheduler", apolloScheduler)     //将宿主机ip&port针对某个appid下的操作
	http.HandleFunc("/apolloAdmin/rsServerMsg", rsServerMsg) //管理api，管理rs server msg
	http.HandleFunc("/apolloAdmin/recovery", recovery)       //recovery bad vip:vport

	http.HandleFunc("/apolloAdmin/disable", disablePort) //forbidden api
	http.HandleFunc("/apolloAdmin/enable", enablePort)   //enable port api

	err1 := http.ListenAndServe(":"+port, nil) //设置监听的端口
	if err1 != nil {
		addErrorLog(logId, "ListenAndServe: ", err1)
		errH <- false
	}
}

func disablePort(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	if r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)
	r.ParseForm()
	vip, ipexist := r.Form["vip"]
	if ipexist != true {
		http.Error(w, `{"result":"1"}`, http.StatusBadRequest)
		return
	}
	vport, portexist := r.Form["vport"]
	if portexist != true {
		http.Error(w, `{"result":"1"}`, http.StatusBadRequest)
		return
	}

	go callRmsChnRsWeight(vip[0], vport[0], "0", logId)
	w.Write([]byte(`{"result":"0"}`))
}

func enablePort(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	if r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)
	r.ParseForm()
	vip, ipexist := r.Form["vip"]
	if ipexist != true {
		http.Error(w, `{"result":"1"}`, http.StatusBadRequest)
		return
	}
	vport, portexist := r.Form["vport"]
	if portexist != true {
		http.Error(w, `{"result":"1"}`, http.StatusBadRequest)
		return
	}

	go callRmsChnRsWeight(vip[0], vport[0], "5", logId)
	w.Write([]byte(`{"result":"0"}`))
}

func callRmsChnRsWeight(vip, vport, weight string, logId int64) error {
	addLog(logId, "callRmsChnRsWeight begin: ", vip, vport)
	chnvar, err := getRsJsonOnlyForEnableAndDisable(vip, vport, weight, logId)
	if err != nil {
		return err
	}
	chksum := getChnRsWeightChksum(vip, chnvar)

	for i := 5; i > 0; i-- {
		requestapi := config["bgw_api_addr"] + rsChnRsWeight + "&department=" + department + "&product=" + productname + "&vip=" + vip + "&chn_rs_var=" + chnvar + "&chksum=" + chksum
		addLog(logId, "call rschnrsweight api:", requestapi)
		var httpReq *http.Request
		client := &http.Client{}
		httpReq, _ = http.NewRequest("GET", requestapi, nil)
		httpResp, cerr := client.Do(httpReq)
		if cerr != nil {
			sentMail("call rschnrsweight api Error!!", requestapi, logId)
			addErrorLog(logId, "get rsChnRsWeight!", requestapi, cerr)
			return cerr
		}
		defer httpResp.Body.Close()
		bodys, _ := ioutil.ReadAll(httpResp.Body)
		addLog(logId, "call rsChnRsWeight result:", string(bodys))
		js, _ := simplejson.NewJson(bodys)
		status, _ := js.Get("status").Int()
		if 0 != status {
			sentMail("call rschnrsweight api result!!", requestapi, string(bodys), logId)
			addErrorLog(logId, "call rsChnRsWeight error happened!", requestapi, string(bodys))
			time.Sleep(time.Second * 20)
			continue
			return errors.New("error happened")
		}
		break
	}
	return nil
}
func getChnRsWeightChksum(vip, rsvar string) string {
	data := department + productname + vip + rsvar + rms_token
	t := md5.New()
	io.WriteString(t, data)
	return fmt.Sprintf("%x", t.Sum(nil))
}

func getRsJsonOnlyForEnableAndDisable(vip string, vport string, weight string, logId int64) (s string, e error) {
	defer func() (string, error) {
		err := recover()
		if err != nil {
			addErrorLog(logId, "getRsJsonOnlyForEnableAndDisable panic error:", err)
			return "", nil
		}
		return "", nil
	}()
	requestapi := config["bgw_api_addr"] + rsGetRsInfoApi + "&vip=" + vip + "&port=" + vport
	addLog(logId, "rs get rs info api:", requestapi)
	var httpReq *http.Request
	client := &http.Client{}
	httpReq, _ = http.NewRequest("GET", requestapi, nil)
	httpResp, err := client.Do(httpReq)
	if err != nil {
		sentMail("call requestapi Error!!", requestapi, logId)
		addErrorLog(logId, "get rsGetRsInfoApi!", requestapi, err)
		return "", err
	}
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)

	js, _ := simplejson.NewJson(bodys)
	port_info_list := js.Get("message").Get("port_info_list")
	arr, _ := port_info_list.Array()
	content, _ := (arr[0]).(map[string]interface{})
	infolist := content["rs_info_list"]
	infolista, _ := infolist.([]interface{})

	newinfolist := make([]interface{}, 0)

	for _, a := range infolista {
		m, _ := a.(map[string]interface{})
		serverid := m["server_id"].(string)
		m["weight"] = weight
		if serverid != disable_server_id {
			newinfolist = append(newinfolist, a)
		}
	}
	content["rs_info_list"] = newinfolist
	arr[0] = content
	outmap := make(map[string]interface{})
	outmap["chn_rs_var"] = arr

	jsonstr, jsonerr := json.Marshal(outmap)
	return string(jsonstr), jsonerr
}

//开启服务，从availPort资源池中分配一个可用的publicip & port
func startService(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	dbconn := initDbConn(logId)
	defer dbconn.Close()
	if r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)
	r.ParseForm()
	platformId, ipexist := r.Form["platformId"]
	if ipexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	initAvailPort(logId)
	var errcheck error
	publicIp, _, _, _, openPort, errcheck := checkRuleNum(dbconn, logId)
	if errcheck != nil {
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		addErrorLog(logId, "check Rule Num Error: ", errcheck)
		return
	}
	addLog(logId, "get available public ip and port is :", publicIp, openPort)

	dbconn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")
	openPortInt, _ := strconv.Atoi(openPort)
	platformIdInt, _ := strconv.Atoi(platformId[0])
	sqli := "INSERT INTO `appidProxyPort` ( `platformId`,`publicIp`, `proxyPort`) VALUES ( ? , ? ,?)"
	stmt, sterr := dbconn.Prepare(sqli)
	if sterr != nil {
		addErrorLog(logId, "appid proxy port insert prepare error : ", sterr)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		return
	}
	if _, err := stmt.Exec(platformIdInt, publicIp, openPortInt); err != nil {
		addErrorLog(logId, "appid proxy port insert error : ", err, platformIdInt, publicIp, openPortInt)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		return
	}
	//更新publicip范围中可用的port数量，具体为+1
	updateBgwAvailPortNum(dbconn, publicIp, openPortInt, true, logId)
	dbconn.Exec("COMMIT")
	addLog(logId, "COMMIT", sqli, platformIdInt, publicIp, openPortInt)
	w.Write([]byte(`{"result":"1","platformId":"` + platformId[0] + `","publicIp":"` + publicIp + `","publicPort":"` + openPort + `"}`))
}

func deleteService(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	dbconn := initDbConn(logId)
	defer dbconn.Close()
	if r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)

	r.ParseForm()
	publicIp, ipexist := r.Form["publicIp"]
	publicPort, portexist := r.Form["publicPort"]
	if ipexist != true || portexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}

	sql := "select appid,internalPort from appidProxyPort where publicIp = '" + publicIp[0] + "' and proxyPort = " + publicPort[0]
	rows, errc := dbconn.Query(sql)
	if errc != nil {
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		addErrorLog(logId, "check appid,internalPort if exist error:", sql)
		return
	}
	var appiddb string
	var interportdb int
	for rows.Next() {
		if err := rows.Scan(&appiddb, &interportdb); err == nil {
			break
		}
	}
	if appiddb != "0" || interportdb != 0 {
		http.Error(w, `{"result":"0"}`, http.StatusForbidden)
		addErrorLog(logId, "in deleteService selected publicip & publicport error!", publicIp, publicPort, appiddb, interportdb)
		return
	}

	dbconn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")
	openPortInt, _ := strconv.Atoi(publicPort[0])
	sqli := "delete from `appidProxyPort` where publicIp= ? and proxyPort= ?"
	stmt, sterr := dbconn.Prepare(sqli)
	if sterr != nil {
		addErrorLog(logId, "appid proxy port delete prepare error : ", sterr)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		return
	}
	if _, err := stmt.Exec(publicIp[0], openPortInt); err != nil {
		addErrorLog(logId, "appid proxy port delete error : ", err, publicIp, openPortInt)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		return
	}
	//更新publicip范围中可用的port数量，具体为-1
	updateBgwAvailPortNum(dbconn, publicIp[0], openPortInt, false, logId)
	dbconn.Exec("COMMIT")
	addLog(logId, "COMMIT", sqli, publicIp, openPortInt)
	portAvail[publicIp[0]][openPortInt] = true
	w.Write([]byte(`{"result":"1"}`))
}

func serviceCheck(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	dbconn := initDbConn(logId)
	defer dbconn.Close()
	if r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)

	r.ParseForm()
	appid, idexist := r.Form["appid"]
	publicIp, ipexist := r.Form["publicIp"]
	publicPort, portexist := r.Form["publicPort"]
	if idexist != true || ipexist != true || portexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}

	sql := "select count(*) from port_proxy_rule where appid='" + appid[0] + "' and publicIp='" + publicIp[0] + "' and proxyPort='" + publicPort[0] + "' and bgwact = '1'"
	rows, errc := dbconn.Query(sql)
	if errc != nil {
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		addErrorLog(logId, "check appid,internalPort if exist error:", sql)
		return
	}
	var num int
	for rows.Next() {
		if err := rows.Scan(&num); err == nil {
			break
		}
	}
	if num > 0 {
		w.Write([]byte(`1`))
	} else {
		w.Write([]byte(`0`))
	}
}

//revocery api
func recovery(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	questData, err := getQuestData(r, logId)
	if debug == 1 {
		fmt.Println("request data is :", questData, " if has error:", err)
	}
	if err != nil {
		http.Error(w, `{"result":"0","msg":"`+err.Error()+`"}`, http.StatusBadRequest)
		addErrorLog(logId, "recovery: ", err)
		return
	}
	addLog(logId, "quest data is:", questData)
	appid, idexist := questData["appid"]
	if idexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "get param appid error!")
		return
	}
	interport, interexist := questData["interport"]
	if interexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "get param interexist error!")
		return
	}
	vip, ipexist := questData["vip"]
	if ipexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "get param vip error!")
		return
	}
	vport, ptexist := questData["vport"]
	if ptexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "get param vport error!")
		return
	}

	dbconn := initDbConn(logId)
	defer dbconn.Close()
	vport_int, _ := strconv.Atoi(vport)
	dbconn.Exec("START TRANSACTION")
	sqld := "delete from port_proxy_rule where publicIp = ? and proxyPort = ?"
	if debug == 1 {
		fmt.Println("delete sql:", sqld, vip, vport)
	}
	stmt, _ := dbconn.Prepare(sqld)
	if _, err := stmt.Exec(vip, vport_int); err != nil {
		addErrorLog(logId, "recovery!! deleteRuleFrom port_proxy_rule delete error : ", err, vip, vport_int)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0","error_msg":"deleteRuleFrom port_proxy_rule delete error"}`, http.StatusInternalServerError)
	}
	sqlu := "update appidProxyPort set appid = 0,internalPort=0,avail=0 where publicIp = ? and proxyPort = ?"
	if debug == 1 {
		fmt.Println("update sql :", sqlu, vip, vport)
	}
	stmtu, _ := dbconn.Prepare(sqlu)
	if _, err = stmtu.Exec(vip, vport_int); err != nil {
		addErrorLog(logId, "recovery!! updateRuleFrom appidProxyPort update error : ", err, vip, vport_int)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0","error_msg":"updateRuleFrom appidProxyPort update error"}}`, http.StatusInternalServerError)
	}
	schedulerDelete(appid, interport, logId)
	dbconn.Exec("COMMIT")
	w.Write([]byte(`{"result":"1"}`))
}

//将appid对应的应用针对某个publicip&port下的操作
func apolloService(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	if r.Method != "POST" && r.Method != "DELETE" && r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)
	questData, err := getQuestData(r, logId)
	if err != nil {
		http.Error(w, `{"result":"0","msg":"`+err.Error()+`"}`, http.StatusBadRequest)
		addErrorLog(logId, "apolloService: ", err)
		return
	}
	addLog(logId, "quest data is:", questData)
	appid, idexist := questData["appid"]
	if idexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "get param appid error!")
		return
	}
	port, ptexist := questData["port"]
	if ptexist != true && r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "get param port error!")
		return
	}
	dbconn := initDbConn(logId)
	defer dbconn.Close()
	if r.Method == "POST" {
		proto, pexist := questData["proto"]
		publicIp, pipex := questData["publicIp"]
		publicPort, portex := questData["publicPort"]
		if pexist != true || pipex != true || portex != true || (proto != "tcp" && proto != "udp") {
			http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
			addErrorLog(logId, "get param proto error!")
			return
		}
		portInt, _ := strconv.Atoi(port)
		sqlc := "select count(*) from appidProxyPort where appid = '" + appid + "' and internalPort = " + port
		rows, errc := dbconn.Query(sqlc)
		if errc != nil {
			http.Error(w, `{"result":"0"}`, http.StatusForbidden)
			addErrorLog(logId, "check appid if exist error:", errc)
			return
		}
		var appnum int
		for rows.Next() {
			if err := rows.Scan(&appnum); err == nil {
				break
			}
		}
		if appnum > 0 {
			http.Error(w, `{"result":"0"}`, http.StatusForbidden)
			addErrorLog(logId, "The appid has already exist!", appid, proto, port)
			return
		}

		//check publicip,proxyport unique
		sql2 := "select appid,internalPort from appidProxyPort where publicIp = '" + publicIp + "' and proxyPort = " + publicPort
		rows, errc = dbconn.Query(sql2)
		if errc != nil {
			http.Error(w, `{"result":"0"}`, http.StatusForbidden)
			addErrorLog(logId, "check appid,internalPort if exist error:")
			return
		}
		var appiddb string
		var interportdb int
		for rows.Next() {
			if err := rows.Scan(&appiddb, &interportdb); err == nil {
				break
			}
		}
		if appiddb != "0" || interportdb != 0 {
			http.Error(w, `{"result":"0"}`, http.StatusForbidden)
			addErrorLog(logId, "in apolloService selected publicip & publicport error!", publicIp, publicPort, appiddb, interportdb)
			return
		}

		if !checkAppidLock(appid, logId) {
			addErrorLog(logId, "appid has locked : ", appid)
			http.Error(w, `{"result":"3"}`, http.StatusInternalServerError)
			return
		}

		// get available port
		dbconn.Exec("START TRANSACTION")
		addLog(logId, "START TRANSACTION")
		openPortInt, _ := strconv.Atoi(publicPort)
		//sqli := "INSERT INTO `appidProxyPort` (`id`, `appid`, `publicIp`, `proxyPort`, `internalPort`) VALUES (NULL, ?,?,?,?);"
		sqli := "update appidProxyPort set appid=? , internalPort=?, avail = ? where publicIp = ? and proxyPort = ?"
		stmt, _ := dbconn.Prepare(sqli)
		if _, err := stmt.Exec(appid, portInt, 1, publicIp, openPortInt); err != nil {
			addErrorLog(logId, "update proxy port insert error : ", err)
			dbconn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return
		}
		if debug == 1 {
			err = nil
		} else {
			err = schedulerCreate(appid, port, proto, logId)
		}
		if err != nil {
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			addErrorLog(logId, "schedulerCreateError: ", err)
			dbconn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return
		}
		dbconn.Exec("COMMIT")
		addLog(logId, "COMMIT", sqli, appid, publicIp, openPortInt, portInt)

		w.Write([]byte(`{"result":"1"}`))
	} else if r.Method == "DELETE" {
		errdelete := deleteRuleByAppid(dbconn, questData, logId)
		if errdelete != nil {
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			addErrorLog(logId, "delete rule Error: ", errdelete)
			return
		}
		if debug == 1 {
			err = nil
		} else {
			err = schedulerDelete(appid, port, logId)
		}
		if err != nil {
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			addErrorLog(logId, "schedulerDeleteError: ", err)
			return
		}
		w.Write([]byte(`{"result":"1"}`))
	} else if r.Method == "GET" {
		output, errget := getRule(dbconn, questData, logId)
		if errget != nil {
			http.Error(w, `{"result":"0","msg":"`+errget.Error()+`"}`, http.StatusInternalServerError)
			addErrorLog(logId, "get rule Error: ", errget)
			return
		}
		w.Write([]byte(output))
	}
}

func rsServerMsg(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" && r.Method != "DELETE" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	addLog(-1, "request body data is :", string(body))
	if err != nil {
		return
	}
	var retVal []string
	json.Unmarshal(body, &retVal)
	if len(retVal) <= 0 {
		addErrorLog(-1, "param error!", retVal)
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	if r.Method == "DELETE" {
		err := deleteRsMsg(retVal)
		if err != nil {
			addErrorLog(-1, "rs msg delete error:", err)
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			return
		}
	}
	if r.Method == "POST" {
		err := addRsMsg(string(body))
		if err != nil {
			addErrorLog(-1, "rs msg add error:", err)
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			return
		}
	}
	w.Write([]byte(`{"result":"1"}`))
}

func apolloScheduler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	questData, err := getSchedulerData(r, 0)
	if err != nil {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(0, "getSchedulerData: ", err)
		return
	}
	logId := questData.Logid
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)

	appid := questData.Appid
	//检查当前此appid是否上锁，如上锁则不能做处理
	if !checkAppidLock(appid, logId) {
		http.Error(w, `{"result":"2"}`, http.StatusBadRequest)
		addErrorLog(logId, "appid has locked: ", appid)
		return
	}
	addLog(logId, "quest data is:", questData, appid)
	//异步为appid下对应的rs server进行管理
	go updateBgwRules(appid, questData, logId)
	w.Write([]byte(`{"result":"1"}`))
}

func updateBgwRules(appid string, scheobj schedulerObject, logId int64) error {
	conn := initDbConn(logId)
	defer conn.Close()
	//在处理前先对此appid上锁，完成后在解锁
	lockAppidAct(appid, logId)
	defer unlockAppidAct(appid, logId)
	addLog(logId, "updateBgwrules scheobj msg:", scheobj)

	sqls := "select publicIp,proxyPort,internalPort from appidProxyPort where appid = '" + appid + "'"
	rowget, errc := conn.Query(sqls)
	if errc != nil {
		addErrorLog(logId, "check publicip,publicport if exist error:", sqls)
		return errc
	}
	var publicip string
	var proxyport, internalport int
	rulePubIp := make(map[string]string) //ui ip&port => public ip
	rulePubPort := make(map[string]int)  //ui ip&port => public port
	ruleMap := make(map[string]bool)
	interportPubIp := make(map[int]string) //inter port => public ip
	interportPubPort := make(map[int]int)  //inter port => public port
	for rowget.Next() {
		if err := rowget.Scan(&publicip, &proxyport, &internalport); err == nil {
			interportPubIp[internalport] = publicip
			interportPubPort[internalport] = proxyport
			sql := "select uiIpPort from port_proxy_rule where publicIp = '" + publicip + "' and proxyPort = '" + strconv.Itoa(proxyport) + "'"
			rows, err := conn.Query(sql)
			if err != nil {
				addErrorLog(logId, "select bgw rule error:", err, sql)
				return err
			}
			addLog(logId, "select bgw rule ,get sql:", sql)
			var uiipport string
			for rows.Next() {
				if err := rows.Scan(&uiipport); err == nil {
					ruleMap[uiipport] = true
					rulePubIp[uiipport] = publicip
					rulePubPort[uiipport] = proxyport
				} else {
					addErrorLog(logId, "get bgw rule error:", err)
					return err
				}
			}
		} else {
			addErrorLog(logId, "select port_proxy_rule error:", err)
			return err
		}
	}
	if len(interportPubIp) <= 0 {
		addErrorLog(logId, "do not have this appid recode!", appid)
		return errors.New("do not have this appid recode!" + appid)
	}
	addLog(logId, "rulePubIp:", rulePubIp)
	addLog(logId, "rulePubPort:", rulePubPort)
	addLog(logId, "ruleMap:", ruleMap)
	addLog(logId, "interportPubIp:", interportPubIp)
	addLog(logId, "interportPubPort:", interportPubPort)

	delrule := make(map[string]RsInfoObj)
	addrule := make(map[string]RsInfoObj)
	actrule := make(map[string]RsInfoObj)
	ruleProto := make(map[string]string)  //ui ip&port => proto
	ruleInterport := make(map[string]int) //ui ip&port => interport
	for _, portType := range scheobj.Ports {
		interport := portType.Internalport
		if _, interexist := interportPubIp[interport]; interexist != true {
			addErrorLog(logId, "the porttype did not exist!", portType)
			continue
		}
		proto := portType.Proto
		for _, ipport := range portType.Ipport {
			hosts := strings.Split(ipport, ":")
			rsinfo, rserr := getRsInfoMsg(conn, hosts[0], logId)
			if rserr != nil {
				addErrorLog(logId, "getRsInfoMsg error!", rserr)
				continue
			}
			key := rsinfo.Ip + ":" + hosts[1]
			actrule[key] = rsinfo
			ruleProto[key] = proto
			ruleInterport[key] = interport
		}
	}
	addLog(logId, "actrule:", actrule)
	addLog(logId, "ruleProto:", ruleProto)
	addLog(logId, "ruleInterport:", ruleInterport)

	//筛选出将要进行del操作的列表
	for k, _ := range ruleMap {
		if rsi, exist := actrule[k]; exist == false {
			delrule[k] = rsi
		}
	}
	addLog(logId, "delete rule arr is :", delrule)

	//筛选出将要进行add操作的列表
	for k2, rsin := range actrule {
		if _, exis := ruleMap[k2]; exis == false {
			addrule[k2] = rsin
		}
	}
	addLog(logId, "add rule arr is :", addrule)

	conn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")
	sqld := "delete from port_proxy_rule where uiIpPort = ?"
	stmt, _ := conn.Prepare(sqld)

	//进行删除操作，只有bgw先删除ok才更新db
	for uiipp, info := range delrule {
		i := 3
		for ; i > 0; i-- {
			//bgw api的del操作
			rs := deleteBgwRule(conn, rulePubIp[uiipp], rulePubPort[uiipp], uiipp, logId)
			if rs["status"] == "0" {
				addLog(logId, "delete rs bgw rule OK!", sqld, uiipp, info)
				break
			} else {
				sentMail("delete rs bgw rule Error!!", rs["message"], logId)
				addErrorLog(logId, "delete rs bgw rule Error!!", rs, sqld, uiipp, info)
			}
		}
		if i <= 0 {
			addErrorLog(logId, "finally delete rule Error!", sqld, uiipp, info)
			continue
		}
		if _, err := stmt.Exec(uiipp); err == nil {
			addLog(logId, "deleteRulefromSche delete sql: ", sqld, uiipp)
		} else {
			addErrorLog(logId, "deleteRuleFrom port_proxy_rule delete error : ", err, info)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	}
	conn.Exec("COMMIT")
	addLog(logId, "DELETE COMMIT")

	//如果是删除appid的port服务，则回收此appid对应的publicip & port
	deleteAppidProxyPort(conn, appid, logId)

	conn.Exec("START TRANSACTION")
	addLog(logId, "INSERT START TRANSACTION")
	sqla := "INSERT INTO port_proxy_rule ( appid, publicIp, proxyPort, proto, uiIpPort) VALUES ( ?,?,?,?,?)"
	stmta, _ := conn.Prepare(sqla)

	//进行增加操作，只有先更新db ok后才请求bgw接口
	for uiippa, infoa := range addrule {
		if _, err := stmta.Exec(appid, interportPubIp[ruleInterport[uiippa]], interportPubPort[ruleInterport[uiippa]], ruleProto[uiippa], uiippa); err == nil {
			addLog(logId, "addRulefromSche add sql: ", sqla, uiippa)
			i := 3
			for ; i > 0; i-- {
				//bgw api的add操作
				rs := addBgwRule(interportPubIp[ruleInterport[uiippa]], interportPubPort[ruleInterport[uiippa]], infoa, uiippa, logId)
				if rs["status"] == "0" {
					addLog(logId, "add rs bgw rule OK!", sqla, uiippa, infoa)
					sqluprule := "UPDATE port_proxy_rule SET bgwact = '1' WHERE appid = '" + appid + "' and publicIp = '" + interportPubIp[ruleInterport[uiippa]] + "' and proxyPort = '" + strconv.Itoa(interportPubPort[ruleInterport[uiippa]]) + "'"
					_, uprerr := conn.Exec(sqluprule)
					addLog(logId, "update port proxy rule bgw act result. \t ", sqluprule, uprerr)
					break
				} else {
					sentMail("add rs bgw rule Error!!", rs["message"], logId)
					addErrorLog(logId, "add rs bgw rule Error!!", rs, sqla, uiippa, infoa)
				}
			}
			if i <= 0 {
				addErrorLog(logId, "finally add rule Error!", sqla, uiippa, infoa)
			}
		} else {
			addErrorLog(logId, "addRuleFrom port_proxy_rule add error : ", err, infoa)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	}

	conn.Exec("COMMIT")
	addLog(logId, "INSERT COMMIT")
	return nil
}

//删除db中存储的server的rs msg信息
func deleteRsMsg(serverArr []string) error {
	conn := initDbConn(-1)
	defer conn.Close()
	serverlist := "("
	for _, s := range serverArr {
		serverlist = serverlist + "'" + s + "',"
	}
	serverlist = serverlist + "'1')"
	sqld := "delete from serverRsInfoMsg where host in " + serverlist
	_, err := conn.Exec(sqld)
	return err
}

//增加db中存储的server的rs msg信息
func addRsMsg(serverlist string) error {
	var httpReq *http.Request
	client := &http.Client{}
	requestapi := rsInfoApi + serverlist
	httpReq, _ = http.NewRequest("GET", requestapi, nil)
	httpResp, err := client.Do(httpReq)
	if err != nil {
		addErrorLog(-1, "get rs msg error!", requestapi, err)
		return err
	}
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	addLog(-1, requestapi, httpResp.Status, httpResp.Proto, string(bodys))
	var result RsInfo
	json.Unmarshal(bodys, &result)
	addLog(-1, "get rs Info Msg:", result)
	if result.Status == 1 {
		addErrorLog(-1, "get rs msg error!", requestapi, string(bodys), err)
		return errors.New("get rs info msg : somethings error!")
	}

	conn := initDbConn(-1)
	defer conn.Close()
	server := []string{}
	for _, rso := range result.Message {
		server = append(server, "('"+rso.Ip+"','"+rso.Id+"','"+rso.Host+"','"+rso.Type+"')")
	}

	sqli := "INSERT INTO serverRsInfoMsg (ip, id, host, type) VALUES " + strings.Join(server, ",")
	_, err = conn.Exec(sqli)
	addLog(-1, "insert rs Info Msg:", sqli, err)
	return err
}

//获取db中存储的server的rs msg信息
func getRsInfoMsg(conn *sql.DB, hostip string, logId int64) (RsInfoObj, error) {
	sql := "select id,host,type from serverRsInfoMsg where ip = '" + hostip + "'"
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(logId, "select server rs infomsg error:", err, sql)
		return RsInfoObj{}, err
	}
	addLog(logId, "select server rs infomsg ,get sql:", sql)
	var hostname, typename string
	var id int
	hasr := 0
	for rows.Next() {
		if err := rows.Scan(&id, &hostname, &typename); err == nil {
			hasr = 1
		} else {
			addErrorLog(logId, "get server rs infomsg error:", err)
			return RsInfoObj{}, err
		}
	}
	if hasr == 0 {
		cmd := exec.Command("host", hostip)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Run()
		fields := strings.Fields(out.String())
		hostname := strings.Trim(fields[len(fields)-1], ".baidu.com.")
		adderr := addRsMsg(`["` + hostname + `"]`)
		if adderr != nil {
			sentMail("[getRsInfoMsg] addRsMsg error:", hostname, adderr.Error(), logId)
			return RsInfoObj{}, errors.New("add new recode error!" + hostip)
		}
		rows, _ = conn.Query(sql)
		for rows.Next() {
			if err = rows.Scan(&id, &hostname, &typename); err != nil {
				addErrorLog(logId, "get server rs infomsg error:", err)
				return RsInfoObj{}, err
			}
		}
	}

	return RsInfoObj{Host: hostname,
		Id:   strconv.Itoa(id),
		Ip:   hostip,
		Type: typename}, nil
}

//操作bgw api进行del rule操作
func deleteBgwRule(conn *sql.DB, pubip string, port int, uiipport string, logId int64) map[string]string {
	hosts := strings.Split(uiipport, ":")
	rsobj, errhas := getRsInfoMsg(conn, hosts[0], logId)
	if errhas != nil {
		addErrorLog(logId, "in the deleteBgwRule get rs info msg error!", hosts, errhas)
		return make(map[string]string)
	}
	requestapi := rsActApi + `&vip=` + pubip + `&vport=` + strconv.Itoa(port) + `&del_rs=[{"id":"` + rsobj.Id + `","ip":"` + rsobj.Ip + `","service_port":"` + hosts[1] + `","type":"` + rsobj.Type + `"}]`
	if debug == 1 {
		fmt.Println("deleteBgwRule:", requestapi)
	}
	checkApi := rsCheckRsApi + `&vip=` + pubip + `&vport=` + strconv.Itoa(port)
	bodys, err := basicGetUrl(requestapi)
	if debug == 1 {
		fmt.Println("deleteBgwRule:", string(bodys), err)
	}
	if err != nil {
		addErrorLog(logId, "request error!", requestapi, err)
		return make(map[string]string)
	}
	addLog(logId, "delete rs url request:", requestapi, string(bodys))
	var objmap map[string]*json.RawMessage
	unjsonErr := json.Unmarshal(bodys, &objmap)
	if unjsonErr != nil {
		addErrorLog(logId, "delete rs url unjson error:", unjsonErr)
	}
	var status int
	var error_code int = 0
	var msg string
	json.Unmarshal(*objmap["status"], &status)
	json.Unmarshal(*objmap["message"], &msg)
	result := make(map[string]string)
	result["status"] = strconv.Itoa(status)
	result["message"] = msg
	if status != 0 {
		json.Unmarshal(*objmap["error_code"], &error_code)
		result["status"], result["message"] = errorFixes(error_code, requestapi, checkApi, msg, logId)
		addErrorLog(logId, "deleteBgwRule status != 0:", error_code, result)
	}
	return result
}

//操作bgw api进行add rule操作
func addBgwRule(pubip string, port int, rsobj RsInfoObj, uiipport string, logId int64) map[string]string {
	hosts := strings.Split(uiipport, ":")
	requestapi := rsActApi + `&vip=` + pubip + `&vport=` + strconv.Itoa(port) + `&add_rs=[{"id":"` + rsobj.Id + `","ip":"` + rsobj.Ip + `","service_port":"` + hosts[1] + `","type":"` + rsobj.Type + `","weight":"5"}]`
	if debug == 1 {
		fmt.Println("addBgwRule:", requestapi)
	}
	checkApi := rsCheckRsApi + `&vip=` + pubip + `&vport=` + strconv.Itoa(port)
	bodys, err := basicGetUrl(requestapi)
	if debug == 1 {
		fmt.Println("addBgwRule:", string(bodys), err)
	}
	if err != nil {
		addErrorLog(logId, "request error!", requestapi, err)
		return make(map[string]string)
	}
	addLog(logId, "add rs url request:", requestapi, string(bodys))
	var objmap map[string]*json.RawMessage
	unjsonErr := json.Unmarshal(bodys, &objmap)
	if unjsonErr != nil {
		addErrorLog(logId, "add rs url unjson error:", unjsonErr)
	}
	var status int
	var error_code int = 0
	var msg string
	json.Unmarshal(*objmap["status"], &status)
	if status != 0 {
		json.Unmarshal(*objmap["error_code"], &error_code)
	}
	json.Unmarshal(*objmap["message"], &msg)
	result := make(map[string]string)
	result["status"] = strconv.Itoa(status)
	result["message"] = msg
	if status != 0 {
		json.Unmarshal(*objmap["error_code"], &error_code)
		result["status"], result["message"] = errorFixes(error_code, requestapi, checkApi, msg, logId)
		addErrorLog(logId, "addBgwRule status != 0:", error_code, result)
	}
	return result
}

//根据错误码做出对应策略
func errorFixes(error_code int, api, check_api, err_msg string, logId int64) (string, string) {
	var status, message string
	if debug == 1 {
		error_code = test_error_code
	}
	addErrorLog(logId, "errorFixes check:", error_code, api, check_api, err_msg)
	switch error_code {
	case 1101: //权限问题-REMOTE_ADDR为空
		status = "1"
		message = "权限问题-REMOTE_ADDR为空"
		break
	case 1102: //权限问题-接口的来源不合法
		status = "1"
		message = "权限问题-接口的来源不合法"
		break
	case 1201: //参数异常-IP信息异常
		status = "1"
		message = "参数异常-IP信息异常"
		break
	case 1202: //参数异常-端口信息异常
		status = "1"
		message = "参数异常-端口信息异常"
		break
	case 1203: //参数异常-集群信息异常
		status = "1"
		message = "参数异常-集群信息异常"
		break
	case 1204: //参数异常-RS信息异常
		status = "1"
		message = "参数异常-RS信息异常"
		break
	case 1402: //操作失败-超时
		fallthrough
	case 1301: //函数调用间隔太短
		var retryError bool = true
		for i := 5; i > 0; i-- {
			time.Sleep(time.Second * 10)
			bodys, err := basicGetUrl(api)
			if err != nil {
				continue
			}
			addLog(logId, "errorFixes:", api, string(bodys))
			var objmap map[string]*json.RawMessage
			json.Unmarshal(bodys, &objmap)
			var restatus int
			json.Unmarshal(*objmap["status"], &restatus)
			if restatus != 0 {
				var re_error_vode int
				json.Unmarshal(*objmap["error_code"], &re_error_vode)
				addErrorLog(logId, error_code, "error code : retry request error!", api, string(bodys))
				if re_error_vode == 1402 || re_error_vode == 1301 {
					continue
				} else {
					break
				}
			}
			retryError = false
			break
		}
		if retryError == true {
			status = "1"
			message = " error code:" + strconv.Itoa(error_code)
			addErrorLog(logId, "Retry severral times, error also happened!", api, message)
			sentMail(message, "Retry severral times, error also happened!", api, logId)
		} else {
			status = "0"
			message = "retry ok"
		}
		break
	case 1401: //操作失败-RMS内部问题
		status = "1"
		message = err_msg
		time.Sleep(time.Second * 3)
		addErrorLog(logId, "bgw server error happened!", api, err_msg)
		sentMail(message, "bgw server error happened!", api, logId)
		break
	}
	return status, message
}

//基本的get url的函数
func basicGetUrl(api string) ([]byte, error) {
	var httpReq *http.Request
	client := &http.Client{}
	httpReq, _ = http.NewRequest("GET", api, nil)
	httpResp, err := client.Do(httpReq)
	defer httpResp.Body.Close()
	if err != nil {
		return []byte{}, err
	}
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	return bodys, err
}

//读取调度请求的api数据
func getSchedulerData(r *http.Request, logId int64) (schedulerObject, error) {
	body, err := ioutil.ReadAll(r.Body)
	addLog(logId, "Scheduler body data is :", string(body))
	if err != nil {
		return schedulerObject{}, err
	}
	var retVal schedulerObject
	err = json.Unmarshal(body, &retVal)
	if err != nil {
		addErrorLog(logId, "receive msg error:", err, string(body))
		return retVal, err
	}
	return retVal, nil
}

//将appid进行锁定
func lockAppidAct(appid string, logId int64) error {
	conn := initDbConn(logId)
	defer conn.Close()
	sqli := "INSERT INTO `appidActLock` (`appid`, `lock`) VALUES ( ? , ? )"
	stmt, preerr := conn.Prepare(sqli)
	if preerr != nil {
		addErrorLog(logId, "conn prepare error : ", preerr, appid)
		return preerr
	}
	if result, err := stmt.Exec(appid, 1); err == nil {
		if id, err := result.LastInsertId(); err == nil {
			addLog(logId, "lock appid : ", id, appid)
		} else {
			addErrorLog(logId, "lock appid error : ", err, appid)
			return err
		}
	} else {
		addErrorLog(logId, "lock appid act error : ", err, appid)
		return err
	}
	return nil
}

//解锁appid
func unlockAppidAct(appid string, logId int64) error {
	conn := initDbConn(logId)
	defer conn.Close()
	sqli := "delete from appidActLock where appid = ?"
	stmt, _ := conn.Prepare(sqli)
	if _, err := stmt.Exec(appid); err == nil {
		addLog(logId, "unlock appid : ", appid)
	} else {
		addErrorLog(logId, "unlock appid error : ", err, appid)
		return err
	}
	return nil
}

//如果是删除appid的port服务，则回收此appid对应的publicip & port
func deleteAppidProxyPort(conn *sql.DB, appid string, logId int64) error {
	//delete db
	conn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")

	sqlu := "update appidProxyPort set appid = ?, internalPort=? where appid = ? and avail = ?"
	stmt, _ := conn.Prepare(sqlu)
	if result, err := stmt.Exec("0", 0, appid, 0); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			addLog(logId, "update appidProxyPort affected number : ", id, appid)
		} else {
			addErrorLog(logId, " update appidProxyPort error : ", err, appid)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, " update appidProxyPort error : ", err, appid)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}
	conn.Exec("COMMIT")
	addLog(logId, "COMMIT")
	//end
	return nil
}

//更新publicip范围中可用的port数量
func updateBgwAvailPortNum(conn *sql.DB, pubip string, port int, addrule bool, logId int64) {
	//update db
	sqlu := ""
	if addrule {
		sqlu = "update proxyServer set rulesNumber = rulesNumber + 1 where publicIp = ? and portS <= ? and portM >= ?"
	} else {
		sqlu = "update proxyServer set rulesNumber = rulesNumber - 1 where publicIp = ? and portS <= ? and portM >= ?"
	}
	stmt, _ := conn.Prepare(sqlu)
	if result, err := stmt.Exec(pubip, port, port); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			addLog(logId, "proxyServer update id : ", id, sqlu, pubip, port)
		} else {
			addErrorLog(logId, "proxyServer update empty : ", id, sqlu, pubip, port)
			return
		}
	} else {
		addErrorLog(logId, "proxyServer update error : ", sqlu, pubip, port)
		return
	}
	return
}

//false is locked ,true is unlock
func checkAppidLock(appid string, logId int64) bool {
	conn := initDbConn(logId)
	defer conn.Close()
	sqlc := "select count(*) from appidActLock where appid = '" + appid + "'"
	rows, errc := conn.Query(sqlc)
	if errc != nil {
		addErrorLog(logId, "check appid if locked error:", errc)
		return false
	}
	var appnum int
	for rows.Next() {
		if err := rows.Scan(&appnum); err == nil {
			break
		}
	}
	if appnum > 0 {
		addLog(logId, "The appid has already locked!", appid)
		return false
	}

	return true
}

func checkRuleNum(conn *sql.DB, logId int64) (string, string, string, string, string, error) {
	sql := "select publicIp,portS,portM from proxyServer order by (portM-portS-rulesNumber) desc limit 1"
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(logId, "get query checkRuleNum error:", err)
		return "", "", "", "", "", err
	}
	addLog(logId, "check availble port:", sql)

	var pubip, ports, portm string
	hasport := false
	for rows.Next() {
		if err := rows.Scan(&pubip, &ports, &portm); err == nil {
			hasport = true
			break
		} else {
			addErrorLog(logId, "get scan checkRuleNum error:", err)
			return "", "", "", "", "", err
		}
	}
	if hasport != true {
		addErrorLog(logId, "no availble port :", sql)
		return "", "", "", "", "", errors.New("no availble port")
	}

	ps, _ := strconv.Atoi(ports)
	pm, _ := strconv.Atoi(portm)
	//get port by queue
	hasport = false
	for k, _ := range portAvail[pubip] {
		if k >= ps && k <= pm {
			ps = k
			hasport = true
			break
		}
	}
	if hasport != true {
		addErrorLog(logId, "no availble port,need restart! :", sql)
		return "", "", "", "", "", errors.New("no availble port,need restart!")
	}

	delete(portAvail[pubip], ps)
	return pubip, ports, portm, "", strconv.Itoa(ps), nil
}

func deleteRuleByAppid(conn *sql.DB, data map[string]string, logId int64) error {
	appid := data["appid"]
	interport := data["port"]
	sql := `select publicIp,proxyPort
			from appidProxyPort
			where appid='` + appid + `' and internalPort = ` + interport
	rows, err := conn.Query(sql)
	addLog(logId, "select delete rules:", sql)
	if err != nil {
		addErrorLog(logId, "sql delete rule error:", sql, err)
		return err
	}
	var pubIp string
	var openPort int
	hasrows := false
	for rows.Next() {
		if err := rows.Scan(&pubIp, &openPort); err == nil {
			hasrows = true
			addLog(logId, "get delete rule:", pubIp, openPort)
		} else {
			addErrorLog(logId, "delete select error : ", err, appid, pubIp, openPort)
			return err
		}
	}
	if hasrows == false {
		addLog(logId, "do not have this recode!!", sql, appid, interport)
		return errors.New("do not have this recode!!")
	}
	//delete db
	conn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")

	//sqlu := "update appidProxyPort set appid = ?, internalPort=? where publicIp = ? and proxyPort = ?"
	sqlu := "update appidProxyPort set avail = ? where publicIp = ? and proxyPort = ?"
	stmt, _ := conn.Prepare(sqlu)
	if result, err := stmt.Exec(0, pubIp, openPort); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			addLog(logId, "update appidProxyPort affected number : ", id, pubIp, openPort)
		} else {
			addErrorLog(logId, " update appidProxyPort error : ", err, pubIp, openPort)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, " update appidProxyPort error : ", err, pubIp, openPort)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}
	conn.Exec("COMMIT")
	addLog(logId, "COMMIT")

	return err
}

func getRule(conn *sql.DB, data map[string]string, logId int64) (string, error) {
	appid := data["appid"]
	//port := data["port"]
	sql := "select distinct a.publicIp,a.proxyPort,a.appid,a.internalPort,a.avail,b.proto from appidProxyPort a left join port_proxy_rule b on a.appid = b.appid where a.platformId ='" + appid + "'"
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(logId, "sql get rule error:", err, sql)
		return "", err
	}
	var result []map[string]string
	var pubip, pubport, baeid, interport, avail string
	var proto []byte
	for rows.Next() {
		if err := rows.Scan(&pubip, &pubport, &baeid, &interport, &avail, &proto); err == nil {
			rules := make(map[string]string)
			rules["baeappid"] = baeid
			rules["interport"] = interport
			rules["publicIp"] = pubip
			if string(proto) == "" {
				rules["type"] = "tcp"
			} else {
				rules["type"] = string(proto)
			}
			rules["publicPort"] = pubport
			rules["avail"] = avail
			result = append(result, rules)
		} else {
			addErrorLog(logId, "sql get rule row scan error:", err, sql)
		}
	}

	outjson, jsonerr := json.Marshal(result)
	return string(outjson), jsonerr
}

func schedulerCreate(appid, port, proto string, logId int64) (err error) {
	//return nil

	var httpReq *http.Request
	client := &http.Client{}
	httpReq, _ = http.NewRequest("POST", incApi, strings.NewReader(`{"appid":"`+appid+`","internalport":"`+port+`","porttype":"`+proto+`","vcode":"`+vcode+`"}`))
	httpReq.Host = "newsch.bce.duapp.com"
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpResp, err := client.Do(httpReq)
	if err != nil {
		addErrorLog(logId, "request error!", incApi, err)
		return err
	}
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	addLog(logId, incApi, httpResp.Status, httpResp.Proto, string(bodys))
	result := make(map[string]string)
	json.Unmarshal(bodys, &result)
	if result["retmsg"] != "ok" {
		sentMail("[schedulerCreate] error!", appid, port, proto, incApi, logId)
		return errors.New("url request error!" + incApi)
	}
	return nil
}

func schedulerDelete(appid, port string, logId int64) error {
	var httpReq *http.Request
	client := &http.Client{}
	httpReq, _ = http.NewRequest("POST", decApi, strings.NewReader(`{"appid":"`+appid+`","internalport":"`+port+`","vcode":"`+vcode+`"}`))
	httpReq.Host = "newsch.bce.duapp.com"
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpResp, err := client.Do(httpReq)
	if err != nil {
		addErrorLog(logId, "request error!", decApi, err)
		return err
	}
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	if debug == 1 {
		fmt.Println("schedulerDelete result:", string(bodys))
	}
	addLog(logId, decApi, httpResp.Status, httpResp.Proto, string(bodys))
	result := make(map[string]string)
	json.Unmarshal(bodys, &result)
	if result["retmsg"] != "ok" {
		sentMail("[schedulerDelete] error!", result["retmsg"], appid, port, decApi, logId)
		//return errors.New("url request error!" + decApi)
	}
	return nil
}

func getQuestData(r *http.Request, logId int64) (map[string]string, error) {
	body, err := ioutil.ReadAll(r.Body)
	addLog(logId, "request body data is :", string(body))
	if err != nil {
		return make(map[string]string), err
	}
	var retVal map[string]string
	err = json.Unmarshal(body, &retVal)
	if err != nil {
		addErrorLog(logId, "receive msg error:", err, string(body))
		return retVal, err
	}
	_, erre := retVal["appid"]
	if erre != true {
		return retVal, errors.New("error")
	}
	return retVal, nil
}

func addLog(logId int64, arg ...interface{}) {
	arg = append(arg, "[logId:"+strconv.Itoa(int(logId))+"]")
	//fmt.Println(arg...)
	logNormal.Println(arg...)
}

func addErrorLog(logId int64, arg ...interface{}) {
	arg = append(arg, "[logId:"+strconv.Itoa(int(logId))+"]")
	logError.Println(arg...)
}

func addHeartLog(arg ...interface{}) {
	fmt.Println(arg...)
}

func sentMail(arg ...interface{}) error {
	addLog(-1, "send mail:", arg)
	bodystr := ""
	for _, str := range arg {
		switch str.(type) {
		case float32:
			bodystr += (strconv.Itoa(int(str.(float32))) + "\t")
			break
		case float64:
			bodystr += (strconv.Itoa(int(str.(float64))) + "\t")
			break
		case string:
			bodystr += (str.(string) + "\t")
			break
		case int:
			bodystr += (strconv.Itoa(str.(int)) + "\t")
			break
		case int8:
			bodystr += (strconv.Itoa(str.(int)) + "\t")
			break
		case int16:
			bodystr += (strconv.Itoa(str.(int)) + "\t")
			break
		case int32:
			bodystr += (strconv.Itoa(str.(int)) + "\t")
			break
		case int64:
			bodystr += (strconv.FormatInt(str.(int64), 10) + "\t")
			break
		}
	}
	subject := "Port service Error!"
	body := `
        <html>
        <body>
	<p>` +
		bodystr + `
	</p>
        </body>
        </html>
        `
	if debug == 1 {
		fmt.Println("send mail debug:", mailSendFrom, "", mailHost, mailSendTo)
	}
	err := mail.SendMail(mailSendFrom, "", mailHost, mailSendTo, subject, body, "html")
	if err != nil {
		addErrorLog(-1, "send mail error:", err, arg)
	}
	return err
}

func sentStatisticsMail(bodystr string) error {
	addLog(-1, "sentStatisticsMail:", bodystr)
	subject := "Port service sent statistics mail!"
	body := `
        <html>
        <body>
	<p>` +
		bodystr + `
	</p>
        </body>
        </html>
        `
	if debug == 1 {
		fmt.Println("sent Statistics Mail debug:", mailSendFrom, "", mailHost, mailSendTo)
	}
	err := mail.SendMail(mailSendFrom, "", mailHost, mailSendTo, subject, body, "html")
	if err != nil {
		addErrorLog(-1, "send mail error:", err)
	}
	return err
}

func randNum() int64 {
	return rand.Int63()
}
