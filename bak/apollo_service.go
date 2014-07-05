package main

import (
	//"apollo/amqp"
	. "apollo/apolloRabbitMq"
	"apollo/cfg"
	_ "apollo/mysql"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	//"time"
)

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
var portAvail map[string]map[int]bool
var serverNotifyTime map[string]int
var mq_receive_channel *RabbitMQ
var incApi string = "http://10.81.64.109:8080/app/incPortService"
var decApi string = "http://10.81.64.109:8080/app/decPortService"
var rsInfoApi string = `http://yx-testing-qapool86.yx01.baidu.com/atm-247_BRANCH/?r=interface/api&handler=getRsInfoByHosts&hosts=`
var rsActApi string = `http://yx-testing-qapool86.yx01.baidu.com/atm-247_BRANCH/?r=interface/api&handler=addDelRs&department=MCO`
var vcode string = "qew$%^^21i412o3i4u12(*(*(*)(*)*@*)*)*!"

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get config
	config = make(map[string]string)
	err := cfg.Load("service_config.cfg", config)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(0)
	}

	//open log file
	logNFile, err1 := os.OpenFile(config["normal_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	logEFile, err2 := os.OpenFile(config["error_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	logHFile, err3 := os.OpenFile(config["heart_log"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err1 != nil || err2 != nil || err3 != nil {
		fmt.Println("open log file Error!", err1, err2, err3)
		os.Exit(0)
	}
	logNormal = log.New(logNFile, "\n", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	logError = log.New(logEFile, "\n", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	logHeart = log.New(logHFile, "\n", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	addLog(0, "========================================\n", config)

	initAvailPort()

}
func initAvailPort() {
	conn := initDbConn(0)
	defer conn.Close()
	portAvail = make(map[string]map[int]bool)
	sql1 := "SELECT distinct publicIp,portS,portM  FROM proxyServer"
	rows, err := conn.Query(sql1)
	if err != nil {
		addErrorLog(0, sql1, " initAvailPort error:", err)
		os.Exit(0)
	}
	var pubip, ports, portm string
	for rows.Next() {
		if err := rows.Scan(&pubip, &ports, &portm); err == nil {
			portAvail[pubip] = make(map[int]bool)
			ps, _ := strconv.Atoi(ports)
			pm, _ := strconv.Atoi(portm)
			for ; ps <= pm; ps++ {
				portAvail[pubip][ps] = true
			}
		}
	}

	sql2 := "select distinct publicIP,proxyPort from appidProxyPort"
	rows, err = conn.Query(sql2)
	if err != nil {
		addErrorLog(0, sql2, " initAvailPort error:", err)
		os.Exit(0)
	}
	for rows.Next() {
		if err := rows.Scan(&pubip, &ports); err == nil {
			ps, _ := strconv.Atoi(ports)
			delete(portAvail[pubip], ps)
		}
	}
	return

}
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
	//go checkIsAlive()

	<-handleErr
	addErrorLog(logId, "Error Happended!")
}

func httpHandle(errH chan bool, logId int64) {
	port, err := config["public_api_port"]
	if err != true {
		addErrorLog(logId, err)
		errH <- false
	}
	http.HandleFunc("/startService", startService)
	http.HandleFunc("/deleteService", deleteService)
	http.HandleFunc("/apolloAdmin/rsServerMsg", rsServerMsg)
	http.HandleFunc("/apolloService", apolloService)
	http.HandleFunc("/apolloScheduler", apolloScheduler)
	err1 := http.ListenAndServe(":"+port, nil) //设置监听的端口
	if err1 != nil {
		addErrorLog(logId, "ListenAndServe: ", err1)
		errH <- false
	}
}
func startService(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	dbconn := initDbConn(logId)
	if r.Method != "GET" {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		return
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)
	r.ParseForm()

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
	sqli := "INSERT INTO `apollo`.`appidProxyPort` ( `publicIp`, `proxyPort`) VALUES ( ? , ? )"
	stmt, sterr := dbconn.Prepare(sqli)
	if sterr != nil {
		addErrorLog(logId, "appid proxy port insert prepare error : ", sterr)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		return
	}
	if _, err := stmt.Exec(publicIp, openPortInt); err != nil {
		addErrorLog(logId, "appid proxy port insert error : ", err, publicIp, openPortInt)
		dbconn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
		return
	}
	dbconn.Exec("COMMIT")
	addLog(logId, "COMMIT", sqli, publicIp, openPortInt)
	w.Write([]byte(`{"result":"1","publicIp":"` + publicIp + `","publicPort":"` + openPort + `"}`))
}
func deleteService(w http.ResponseWriter, r *http.Request) {
	logId := randNum()
	dbconn := initDbConn(logId)
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
		addErrorLog(logId, "selected publicip & publicport error!", publicIp, publicPort, appiddb, interportdb)
		return
	}

	dbconn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")
	openPortInt, _ := strconv.Atoi(publicPort[0])
	sqli := "delete from `apollo`.`appidProxyPort` where publicIp= ? and proxyPort= ?"
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
	dbconn.Exec("COMMIT")
	addLog(logId, "COMMIT", sqli, publicIp, openPortInt)
	portAvail[publicIp[0]][openPortInt] = true
	w.Write([]byte(`{"result":"1"}`))
}
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
		addErrorLog(logId, "ListenAndServe: ", err)
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
	if ptexist != true {
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
			addErrorLog(logId, "selected publicip & publicport error!", publicIp, publicPort, appiddb, interportdb)
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
		//sqli := "INSERT INTO `apollo`.`appidProxyPort` (`id`, `appid`, `publicIp`, `proxyPort`, `internalPort`) VALUES (NULL, ?,?,?,?);"
		sqli := "update appidProxyPort set appid=? , internalPort=?, avail = ? where publicIp = ? and proxyPort = ?"
		stmt, _ := dbconn.Prepare(sqli)
		if _, err := stmt.Exec(appid, portInt, 1, publicIp, openPortInt); err != nil {
			addErrorLog(logId, "update proxy port insert error : ", err)
			dbconn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return
		}
		err = schedulerCreate(appid, port, proto, logId)
		if err != nil {
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			addErrorLog(logId, "schedulerCreateError: ", err)
			dbconn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return
		}
		dbconn.Exec("COMMIT")
		addLog(logId, "COMMIT", sqli, appid, publicIp, openPortInt, portInt)

		w.Write([]byte(`{"result":"2"}`))
	} else if r.Method == "DELETE" {
		errdelete := deleteRuleByAppid(dbconn, questData, logId)
		if errdelete != nil {
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			addErrorLog(logId, "delete rule Error: ", errdelete)
			return
		}
		err = schedulerDelete(appid, port, logId)
		if err != nil {
			http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
			addErrorLog(logId, "schedulerDeleteError: ", err)
			return
		}
		w.Write([]byte(`{"result":"2"}`))
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
	if !checkAppidLock(appid, logId) {
		http.Error(w, `{"result":"2"}`, http.StatusBadRequest)
		addErrorLog(logId, "appid has locked: ", appid)
		return
	}
	addLog(logId, "quest data is:", questData, appid)
	go updateBgwRules(appid, questData, logId)
	w.Write([]byte(`{"result":"1"}`))
}
func updateBgwRules(appid string, scheobj schedulerObject, logId int64) error {
	conn := initDbConn(logId)
	defer conn.Close()
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

	for k, _ := range ruleMap {
		if rsi, exist := actrule[k]; exist == false {
			delrule[k] = rsi
		}
	}
	addLog(logId, "delete rule arr is :", delrule)
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
	for uiipp, info := range delrule {
		i := 3
		for ; i > 0; i-- {
			rs := deleteBgwRule(conn, rulePubIp[uiipp], rulePubPort[uiipp], uiipp, logId)
			if rs["status"] == "0" {
				addLog(logId, "delete rs bgw rule OK!", sqld, uiipp, info)
				updateBgwAvailPortNum(conn, rulePubIp[uiipp], rulePubPort[uiipp], false, logId)
				break
			} else {
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

	deleteAppidProxyPort(conn, appid, logId)

	conn.Exec("START TRANSACTION")
	addLog(logId, "INSERT START TRANSACTION")
	sqla := "INSERT INTO port_proxy_rule ( appid, publicIp, proxyPort, proto, uiIpPort) VALUES ( ?,?,?,?,?)"
	stmta, _ := conn.Prepare(sqla)
	for uiippa, infoa := range addrule {
		if _, err := stmta.Exec(appid, interportPubIp[ruleInterport[uiippa]], interportPubPort[ruleInterport[uiippa]], ruleProto[uiippa], uiippa); err == nil {
			addLog(logId, "addRulefromSche add sql: ", sqla, uiippa)
			i := 3
			for ; i > 0; i-- {
				rs := addBgwRule(interportPubIp[ruleInterport[uiippa]], interportPubPort[ruleInterport[uiippa]], infoa, uiippa, logId)
				if rs["status"] == "0" {
					addLog(logId, "add rs bgw rule OK!", sqla, uiippa, infoa)
					updateBgwAvailPortNum(conn, interportPubIp[ruleInterport[uiippa]], interportPubPort[ruleInterport[uiippa]], true, logId)
					break
				} else {
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
		return RsInfoObj{}, errors.New("We do not have this recode!" + hostip)
	}

	return RsInfoObj{Host: hostname,
		Id:   strconv.Itoa(id),
		Ip:   hostip,
		Type: typename}, nil
	/*
		var httpReq *http.Request
		client := &http.Client{}
		requestapi := rsInfoApi + `["` + host + `"]`
		httpReq, _ = http.NewRequest("GET", requestapi, nil)
		httpResp, err := client.Do(httpReq)
		if err != nil {
			addErrorLog(logId, "request error!", requestapi, err)
			return RsInfoObj{}, err
		}
		defer httpResp.Body.Close()
		bodys, _ := ioutil.ReadAll(httpResp.Body)
		addLog(logId, requestapi, httpResp.Status, httpResp.Proto, string(bodys))
		var result RsInfo
		json.Unmarshal(bodys, &result)
		addLog(logId, "get rs Info Msg:", result)
		if result.Status == 0 {
			return result.Message[0], nil
		}
		return RsInfoObj{}, errors.New("get RsInfo Error!")
	*/
}
func deleteBgwRule(conn *sql.DB, pubip string, port int, uiipport string, logId int64) map[string]string {
	hosts := strings.Split(uiipport, ":")
	rsobj, errhas := getRsInfoMsg(conn, hosts[0], logId)
	if errhas != nil {
		addErrorLog(logId, "in the deleteBgwRule get rs info msg error!", hosts, errhas)
		return make(map[string]string)
	}
	var httpReq *http.Request
	client := &http.Client{}
	requestapi := rsActApi + `&vip=` + pubip + `&vport=` + strconv.Itoa(port) + `&del_rs=[{"id":"` + rsobj.Id + `","ip":"` + rsobj.Ip + `","service_port":"` + hosts[1] + `","type":"` + rsobj.Type + `"}]`
	httpReq, _ = http.NewRequest("GET", requestapi, nil)
	httpResp, err := client.Do(httpReq)
	if err != nil {
		addErrorLog(logId, "request error!", requestapi, err)
		return make(map[string]string)
	}
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	addLog(logId, "delete rs url request:", requestapi, httpResp.Status, httpResp.Proto, string(bodys))
	var objmap map[string]*json.RawMessage
	unjsonErr := json.Unmarshal(bodys, &objmap)
	if unjsonErr != nil {
		addErrorLog(logId, "delete rs url unjson error:", unjsonErr)
	}
	var status int
	var msg string
	json.Unmarshal(*objmap["status"], &status)
	json.Unmarshal(*objmap["message"], &msg)
	result := make(map[string]string)
	result["status"] = strconv.Itoa(status)
	result["message"] = msg
	return result
}
func addBgwRule(pubip string, port int, rsobj RsInfoObj, uiipport string, logId int64) map[string]string {
	hosts := strings.Split(uiipport, ":")
	var httpReq *http.Request
	client := &http.Client{}
	requestapi := rsActApi + `&vip=` + pubip + `&vport=` + strconv.Itoa(port) + `&add_rs=[{"id":"` + rsobj.Id + `","ip":"` + rsobj.Ip + `","service_port":"` + hosts[1] + `","type":"` + rsobj.Type + `","weight":"1"}]`
	httpReq, _ = http.NewRequest("GET", requestapi, nil)
	httpResp, err := client.Do(httpReq)
	if err != nil {
		addErrorLog(logId, "request error!", requestapi, err)
		return make(map[string]string)
	}
	defer httpResp.Body.Close()
	bodys, _ := ioutil.ReadAll(httpResp.Body)
	addLog(logId, "add rs url request:", requestapi, httpResp.Status, httpResp.Proto, string(bodys))
	var objmap map[string]*json.RawMessage
	unjsonErr := json.Unmarshal(bodys, &objmap)
	if unjsonErr != nil {
		addErrorLog(logId, "add rs url unjson error:", unjsonErr)
	}
	var status int
	var msg string
	json.Unmarshal(*objmap["status"], &status)
	json.Unmarshal(*objmap["message"], &msg)
	result := make(map[string]string)
	result["status"] = strconv.Itoa(status)
	result["message"] = msg
	return result
}
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
func lockAppidAct(appid string, logId int64) error {
	conn := initDbConn(logId)
	defer conn.Close()
	sqli := "INSERT INTO `apollo`.`appidActLock` (`appid`, `lock`) VALUES ( ? , ? )"
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

//=====================================================
func checkRuleNum(conn *sql.DB, logId int64) (string, string, string, string, string, error) {
	sql := "select publicIp,proxyServerIp,portS,portM from proxyServer where rulesNumber < " + config["proxy_server_port_number"] + " and isAvailable = 1 order by rulesNumber limit 1"
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(logId, "get checkRuleNum error:", err)
		return "", "", "", "", "", err
	}
	addLog(logId, "check availble port:", sql)

	var pubip, proxyip, ports, portm string
	hasport := false
	for rows.Next() {
		if err := rows.Scan(&pubip, &proxyip, &ports, &portm); err == nil {
			hasport = true
			break
		} else {
			addErrorLog(logId, "get checkRuleNum error:", err)
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
	return pubip, ports, portm, proxyip, strconv.Itoa(ps), nil
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
	//end
	return err
}
func getRule(conn *sql.DB, data map[string]string, logId int64) (string, error) {
	appid := data["appid"]
	sql := "select publicIp,proxyPort from port_proxy_rule where appid = '" + appid + "' limit 1"
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(logId, "sql get rule error:", err, sql)
		return "", err
	}
	addLog(logId, "get rule :", sql)
	var pubip, pubport string
	hasRow := false
	for rows.Next() {
		if err := rows.Scan(&pubip, &pubport); err == nil {
			hasRow = true
			break
		}
	}
	rules := make(map[string]string)
	if hasRow == true {
		rules["result"] = "1"
		rules["publicIp"] = pubip
		rules["publicPort"] = pubport
	} else {
		rules["result"] = "0"
	}
	outjson, _ := json.Marshal(rules)
	return string(outjson), nil
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
		return errors.New("url request error!" + incApi)
	}
	return nil
}
func schedulerDelete(appid, port string, logId int64) error {
	//return nil

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
	addLog(logId, decApi, httpResp.Status, httpResp.Proto, string(bodys))
	result := make(map[string]string)
	json.Unmarshal(bodys, &result)
	if result["retmsg"] != "ok" {
		return errors.New("url request error!" + decApi)
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
	fmt.Println(arg...)
}
func addErrorLog(logId int64, arg ...interface{}) {
	arg = append(arg, "[logId:"+strconv.Itoa(int(logId))+"]")
	fmt.Println(arg...)
}
func addHeartLog(arg ...interface{}) {
	fmt.Println(arg...)
}
func randNum() int64 {
	return rand.Int63()
}

/*
func getTaskId(conn *sql.DB, logId int64) int {
	sqlr := "REPLACE INTO taskTicket (stub) VALUES ('a')"
	stmt, _ := conn.Prepare(sqlr)
	if result, err := stmt.Exec(); err == nil {
		if id, err := result.LastInsertId(); err == nil {
			addLog(logId, "get task Id : ", id)
			return int(id)
		} else {
			addErrorLog(logId, "get task Id error:", err)
		}
	} else {
		addErrorLog(logId, "get task Id error:", err)
	}
	return 0
}
func getMqData(logId int64, num int, tidMap map[string]bool) error {
	var err error
	mq_receive_channel, err = initMq(logId)
	if err != nil {
		addErrorLog(logId, "connect reply mq error")
		return err
	}
	defer mq_receive_channel.Close()
	err20 := mq_receive_channel.DeclareQueue(config["reply_queue_name"])
	if err20 != nil {
		addErrorLog(logId, "get data from agent error! ", err20)
	}

	if num != len(tidMap) {
		return errors.New("task number is != mapnumber " + strconv.Itoa(num) + " != " + strconv.Itoa(len(tidMap)))
	}
	message := make(chan []byte, num)
	err = mq_receive_channel.ConsumeQueue(config["reply_queue_name"], message)
	if err != nil {
		addErrorLog(logId, "get queue msg error:", err)
		return err
	}
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(3e9)
		timeout <- true
	}()
	var msg []byte
	addLog(logId, "the total number is :", num)
	for ; num > 0; num-- {
		select {
		case msg = <-message:
			addLog(logId, "queue data is:", string(msg))
			jsonD := jsonToMap(msg)
			addLog(logId, "get queue is :", jsonD)
			if _, exist := tidMap[jsonD["taskid"]]; exist != true {
				addErrorLog(logId, "task id error:", jsonD)
				num++
				continue
			}
			delete(tidMap, jsonD["taskid"])
			if jsonD["result"] != "0" {
				addErrorLog(logId, "agent act error:", jsonD)
				return errors.New("error Happened")
			}
			addLog(logId, "Begin Next")
		case <-timeout:
			addErrorLog(logId, "agent act error timeout:")
			return errors.New("time Out")
		}
	}

	addLog(logId, "agent act OK")
	return nil
}
func jsonToMap(data []byte) map[string]string {
	result := make(map[string]string)
	var retMsg map[string]*json.RawMessage
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal(data, &objmap)
	if err != nil {
		return result
	}
	json.Unmarshal(*objmap["context"], &retMsg)

	var logid, cmd, taskid string
	var res int
	json.Unmarshal(*objmap["logid"], &logid)
	json.Unmarshal(*objmap["cmd"], &cmd)
	json.Unmarshal(*retMsg["taskid"], &taskid)
	json.Unmarshal(*retMsg["result"], &res)
	result["result"] = strconv.Itoa(res)
	result["taskid"] = taskid
	result["logid"] = logid
	result["cmd"] = cmd
	return result
}
func notifyJsonMap(data []byte) map[string]string {
	result := make(map[string]string)
	var retMsg map[string]*json.RawMessage
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal(data, &objmap)
	if err != nil {
		return result
	}
	json.Unmarshal(*objmap["context"], &retMsg)

	var logid, cmd, taskid, host string
	var timestamp int
	json.Unmarshal(*objmap["logid"], &logid)
	json.Unmarshal(*objmap["cmd"], &cmd)
	json.Unmarshal(*retMsg["taskid"], &taskid)
	json.Unmarshal(*retMsg["host"], &host)
	json.Unmarshal(*retMsg["timestamp"], &timestamp)
	result["host"] = host
	result["taskid"] = taskid
	result["logid"] = logid
	result["cmd"] = cmd
	result["timestamp"] = strconv.Itoa(timestamp)
	return result
}
func agentCreate(conn *sql.DB, publicip, pubPortS, pubPortM, proto string, openport int, uiips []string, logId int64) error {
	sql := "select proxyServerIp from proxyServer where publicIp = '" + publicip + "' and portS= " + pubPortS + " and portM=" + pubPortM
	rows, errs := conn.Query(sql)
	if errs != nil {
		addErrorLog(logId, "agentcreate get will create proxy ip error:", errs)
		return errs
	}
	addLog(logId, "agent create sql:", sql)

	var proxyip string
	var proxyips []string
	for rows.Next() {
		if err := rows.Scan(&proxyip); err == nil {
			proxyips = append(proxyips, proxyip)
		}
	}
	addLog(logId, "agent create proxy server:", proxyips)
	uiipstrings := strings.Join(uiips, ",")
	rabbit, err := initMq(logId)
	if err != nil {
		return err
	}
	defer rabbit.Close()

	exerr := rabbit.DeclareExchange(config["send_exchange_name"], "direct", false)
	if exerr != nil {
		addErrorLog(logId, "agentCreate declareexchange Error:", config["send_exchange_name"], exerr)
		//	return exerr
	}
	agentNum := len(proxyips)
	for trynum := 1; trynum <= 3; trynum++ {
		tidMap := make(map[string]bool)
		addLog(logId, "try time number:", trynum)
		for _, v := range proxyips {
			taskId := getTaskId(conn, logId)
			tidMap[strconv.Itoa(taskId)] = true
			sendData := `{"logid":"` + strconv.Itoa(int(logId)) + `","cmd":"add_rule","context":{"taskid":"` + strconv.Itoa(taskId) + `","proto":"` + proto + `","proxy_addr":"` + v + `:` + strconv.Itoa(openport) + `","server_addrs":"` + uiipstrings + `"}}`
			//sendData := `{"logid":"` + strconv.Itoa(int(logId)) + `","cmd":"add_rule","context":{"taskid":"` + strconv.Itoa(taskId) + `","proto":"`+proto+`","proxy_addr":"10.209.78.11:30088","server_addrs":"` + uiipstrings + `"}}`
			if err := rabbit.Publish(config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData); err != nil {
				addErrorLog(logId, "agentCreate publish data Error:", config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData, err)
				//	return err
			}
			addLog(logId, "agentCreate publish data OK:", config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData)
		}
		err = getMqData(logId, agentNum, tidMap)
		if err != nil {
			addErrorLog(logId, "agent error!", err)
			continue
		} else {
			break
		}
	}

	return err
}
func agentDelete(conn *sql.DB, publicip, pubPortS, pubPortM string, openport int, uiips string, logId int64) error {
	//return nil
	sql := "select proxyServerIp from proxyServer where publicIp = '" + publicip + "' and portS= " + pubPortS + " and portM=" + pubPortM
	rows, errs := conn.Query(sql)
	if errs != nil {
		addErrorLog(logId, "agentdelete get will create proxy ip error:", errs)
		return errs
	}
	addLog(logId, "agent delete sql:", sql)

	var proxyip string
	var proxyips []string
	for rows.Next() {
		if err := rows.Scan(&proxyip); err == nil {
			proxyips = append(proxyips, proxyip)
		}
	}
	addLog(logId, "agent delete proxy server:", proxyips)
	uiipstrings := uiips
	rabbit, err := initMq(logId)
	if err != nil {
		return err
	}
	defer rabbit.Close()

	rabbit.DeclareExchange(config["send_exchange_name"], "direct", false)
	agentNum := len(proxyips)
	for trynum := 1; trynum <= 3; trynum++ {
		tidMap := make(map[string]bool)
		addLog(logId, "try time number:", trynum)
		for _, v := range proxyips {
			taskId := getTaskId(conn, logId)
			tidMap[strconv.Itoa(taskId)] = true
			sendData := `{"logid":"` + strconv.Itoa(int(logId)) + `","cmd":"delete_rule","context":{"taskid":"` + strconv.Itoa(taskId) + `","proto":"tcp","proxy_addr":"` + v + `:` + strconv.Itoa(openport) + `","server_addrs":"` + uiipstrings + `"}}`
			if err := rabbit.Publish(config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData); err != nil {
				addErrorLog(logId, "agent Delete publish data Error:", config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData, err)
				//return err
			}
			addLog(logId, "agent Delete publish data OK:", config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData)
		}
		err = getMqData(logId, agentNum, tidMap)
		if err != nil {
			addErrorLog(logId, "agent error!", err)
			continue
		} else {
			break
		}
	}

	return err
}
func agentClear(conn *sql.DB, pubIp, ports, portm string, openPort int, logId int64) error {
	//return nil
	sql := "select proxyServerIp from proxyServer where publicIp = '" + pubIp + "' and portS= " + ports + " and portM=" + portm
	rows, errs := conn.Query(sql)
	if errs != nil {
		addErrorLog(logId, "agent delete get will delete proxy ip error:", errs)
		return errs
	}
	addLog(logId, "agent delete sql:", sql)

	var proxyip string
	var proxyips []string
	for rows.Next() {
		if err := rows.Scan(&proxyip); err == nil {
			proxyips = append(proxyips, proxyip)
		}
	}
	addLog(logId, "agent create proxy server:", proxyips)

	rabbit, err := initMq(logId)
	if err != nil {
		return err
	}
	defer rabbit.Close()

	rabbit.DeclareExchange(config["send_exchange_name"], "direct", false)
	agentNum := len(proxyips)
	for trynum := 1; trynum <= 3; trynum++ {
		tidMap := make(map[string]bool)
		addLog(logId, "try time number:", trynum)
		for _, v := range proxyips {
			taskId := getTaskId(conn, logId)
			tidMap[strconv.Itoa(taskId)] = true
			sendData := `{"logid":"` + strconv.Itoa(int(logId)) + `","cmd":"clear_rule","context":{"taskid":"` + strconv.Itoa(taskId) + `","proto":"tcp","proxy_addr":"` + v + `:` + strconv.Itoa(openPort) + `"}}`
			if err := rabbit.Publish(config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData); err != nil {
				addErrorLog(logId, "agentClear publish data Error:", config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData, err)
				//return err
			}
			addLog(logId, "agentClear publish data OK:", config["send_exchange_name"], "apollo-agent@"+v, config["reply_queue_name"], sendData)
		}
		err = getMqData(logId, agentNum, tidMap)
		if err != nil {
			addErrorLog(logId, "agent error!", err)
			continue
		} else {
			break
		}
	}
	return err
}
func agentAllclear(serverip string) error {
	conn := initDbConn(-1)
	defer conn.Close()
	rabbit, err := initMq(-1)
	if err != nil {
		return err
	}
	defer rabbit.Close()
	taskId := getTaskId(conn, -1)
	tidMap := make(map[string]bool)
	tidMap[strconv.Itoa(taskId)] = true
	sendData := `{"logid":"-1","cmd":"clear_all_rules","context":{"taskid":"` + strconv.Itoa(taskId) + `","proxy_addr":"` + serverip + `"}}`
	addLog(-1, "agent all clear ,send data:", sendData)
	if err := rabbit.Publish(config["send_exchange_name"], "apollo-agent@"+serverip, config["reply_queue_name"], sendData); err != nil {
		addErrorLog(-1, "recover agent Create publish data Error:", config["send_exchange_name"], "apollo-agent@"+serverip, config["reply_queue_name"], sendData, err)
		return err
	}
	err = getMqData(-1, 1, tidMap)
	if err != nil {
		addErrorLog(-1, "all clear rule error!", serverip, err)
		return err
	}
	return nil
}
func notifyRecover(serverip string) error {
	conn := initDbConn(-1)
	defer conn.Close()
	sql := `select appid,proxyPort,proto,uiIpPort
				from port_proxy_rule p inner join proxyServer s
				on p.publicIp=s.publicIp
				where proxyServerIp = '` + serverip + `'
				and proxyPort >=portS and proxyPort <= portM
				order by appid`
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(-1, "notify Recover error:", err, sql)
		return err
	}
	addLog(-1, "notify Recover  ,get sql:", sql)
	var appid, proto, uiip string
	var openport int
	appType := make(map[string]string)
	appRules := make(map[string]string)
	hasrow := false
	for rows.Next() {
		if err := rows.Scan(&appid, &openport, &proto, &uiip); err == nil {
			hasrow = true
			key := serverip + ":" + strconv.Itoa(openport)
			appType[key] = proto
			appRules[key] = appRules[key] + uiip + ","
		} else {
			addErrorLog(-1, "notify Recover error:", err)
			return err
		}
	}
	if hasrow == false {
		addLog(-1, "Recover: this server has no rules!", serverip, sql)
		return nil
	}

	rabbit, err := initMq(-1)
	if err != nil {
		return err
	}
	defer rabbit.Close()
	tidMap := make(map[string]bool)
	for k, v := range appRules {
		taskId := getTaskId(conn, -1)
		tidMap[strconv.Itoa(taskId)] = true
		sendData := `{"logid":"-1","cmd":"add_rule","context":{"taskid":"` + strconv.Itoa(taskId) + `","proto":"` + appType[k] + `","proxy_addr":"` + k + `","server_addrs":"` + strings.Trim(v, ",") + `"}}`
		if err := rabbit.Publish(config["send_exchange_name"], "apollo-agent@"+serverip, config["reply_queue_name"], sendData); err != nil {
			addErrorLog(-1, "recover agent Create publish data Error:", config["send_exchange_name"], "apollo-agent@"+serverip, config["reply_queue_name"], sendData, err)
			return err
		}
		addLog(-1, "recover agent Create publish data OK:", config["send_exchange_name"], "apollo-agent@"+serverip, config["reply_queue_name"], sendData)
	}
	addLog(-1, "all rule seems recover ok!")
	err = getMqData(-1, len(appRules), tidMap)
	if err != nil {
		addErrorLog(-1, "recover answer error!", err)
		return err
	}
	return nil
}
func createSingleRule(conn *sql.DB, appid, pubip, ports, portm, openport, uiip, proto string, logId int64) error {
	oportInt, _ := strconv.Atoi(openport)

	conn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")
	//update db
	ruleNum := 1
	portsi, _ := strconv.Atoi(ports)
	portmi, _ := strconv.Atoi(portm)
	sqlu := "update proxyServer set rulesNumber = rulesNumber + ? where publicIp = ? and portS = ? and portM = ?"
	stmt, _ := conn.Prepare(sqlu)
	if result, err := stmt.Exec(ruleNum, pubip, portsi, portmi); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			addLog(logId, "addRule update id : ", id, sqlu, ruleNum, pubip, ports, portm)
		} else {
			addErrorLog(logId, "addRuleFromSche update error : ", err, id, sqlu, ruleNum, pubip, ports, portm)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, "addRuleFromSche update error : ", err, sqlu, ruleNum, pubip, ports, portm)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}

	sqli := "insert into port_proxy_rule(appid,publicIp,proxyPort,uiIpPort) values(?,?,?,?)"
	stmt, _ = conn.Prepare(sqli)
	if result, err := stmt.Exec(appid, pubip, oportInt, uiip); err == nil {
		if id, err := result.LastInsertId(); err == nil {
			addLog(logId, "addRulefromSche insert id : ", id, appid, pubip, oportInt, uiip)
		} else {
			addErrorLog(logId, "addRuleFromSche insert error : ", err, appid, pubip, oportInt, uiip)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, "addRuleFromSche insert error : ", err, appid, pubip, oportInt, uiip)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}
	conn.Exec("COMMIT")
	addLog(logId, "COMMIT")

	err := agentCreate(conn, pubip, ports, portm, proto, oportInt, []string{uiip}, logId)
	if err != nil {
		addErrorLog(logId, "agent create proxy Error:", appid, pubip, ports, portm, openport, uiip, "  ", err)
		return err
	}
	//end
	return err
}
func deleteRuleByChange(conn *sql.DB, appid, uiip string, logId int64) error {
	sql := "select distinct s.publicIp,proxyPort,portS,portM from port_proxy_rule p inner join proxyServer s where p.proxyPort >= s.portS and p.proxyPort <=s.portM and appid='" + appid + "'"
	rows, err := conn.Query(sql)
	if err != nil {
		addErrorLog(logId, "deleteRuleFromSche error:", err, sql)
		return err
	}
	addLog(logId, "delete rule from sche ,get sql:", sql)
	var pubip, ports, portm string
	var openport int
	hasrow := false
	for rows.Next() {
		if err := rows.Scan(&pubip, &openport, &ports, &portm); err == nil {
			hasrow = true
			break
		} else {
			addErrorLog(logId, "deleteRuleFromSche error:", err)
			return err
		}
	}
	if hasrow == false {
		addErrorLog(logId, "delete rule from sche ,didnot have this recode", appid)
		return errors.New("did not have recode!")
	}
	err = agentDelete(conn, pubip, ports, portm, openport, uiip, logId)
	if err != nil {
		addErrorLog(logId, "agent delete proxy Error:", appid, pubip, ports, portm, openport, uiip, "  ", err)
		return err
	}
	conn.Exec("START TRANSACTION")
	addLog(logId, "START TRANSACTION")
	//update db
	ruleNum := 1
	portsi, _ := strconv.Atoi(ports)
	portmi, _ := strconv.Atoi(portm)
	sqli := "delete from port_proxy_rule where uiIpPort = ?"
	stmt, _ := conn.Prepare(sqli)
	if result, err := stmt.Exec(uiip); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			if id == 0 {
				addErrorLog(logId, "deleteRuleFromSche delete rule is 0")
				conn.Exec("ROLLBACK")
				addLog(logId, "ROLLBACK")
				return errors.New("deleteRuleFromSche delete rule is 0")
			}
			addLog(logId, "deleteRulefromSche insert id : ", id, appid, pubip, openport, uiip)
		} else {
			addErrorLog(logId, "deleteRuleFromSche insert error : ", err, appid, pubip, openport, uiip)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, "deleteRuleFromSche insert error : ", err, appid, pubip, openport, uiip)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}
	sqlu := "update proxyServer set rulesNumber = rulesNumber - ? where publicIp = ? and portS = ? and portM = ?"
	stmt, _ = conn.Prepare(sqlu)
	if result, err := stmt.Exec(ruleNum, pubip, portsi, portmi); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			addLog(logId, "deleteRule update id : ", id, sqlu, ruleNum, pubip, ports, portm)
		} else {
			addErrorLog(logId, "deleteRuleFromSche update error : ", err, id, sqlu, ruleNum, pubip, ports, portm)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, "deleteRuleFromSche update error : ", err, sqlu, ruleNum, pubip, ports, portm)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}

	conn.Exec("COMMIT")
	addLog(logId, "COMMIT")
	//end
	return err
}
func checkIsAlive() error {
	dbconn := initDbConn(-1)
	defer dbconn.Close()
	mq_heart_check, err := initMq(0)
	if err != nil {
		addHeartLog("connect heart mq error")
		return err
	}
	defer mq_heart_check.Close()
	errq := mq_heart_check.DeclareQueue(config["queue_notify"])
	if errq != nil {
		addHeartLog("mq_heart_check declare queue error! ", errq)
	}
	sql := "select distinct proxyServerIp from proxyServer"
	rows, err := dbconn.Query(sql)
	if err != nil {
		addErrorLog(-1, "check is alive get proxyserver error:", err)
		return err
	}

	var proxylist []string
	var proxyip string
	heartChan := make(map[string]chan map[string]string)
	serverNotifyTime = make(map[string]int)
	for rows.Next() {
		if err := rows.Scan(&proxyip); err == nil {
			proxylist = append(proxylist, proxyip)
			serverNotifyTime[proxyip] = 0
			heartChan[proxyip] = make(chan map[string]string, 2)
		}
	}

	addHeartLog("heart check ,proxy server list:", proxylist)
	timecheck, _ := strconv.Atoi(config["check_time"])
	timecheckInt64 := int64(timecheck)
	for _, v := range proxylist {
		go heartCheck(v, heartChan[v], timecheckInt64)
	}
	heartmsg := make(chan []byte)
	for {
		err := mq_heart_check.ConsumeQueue(config["queue_notify"], heartmsg)
		if err != nil {
			addHeartLog("get Heart msg error:", err)
		}
		heartData := <-heartmsg
		tunix := time.Now().Unix()
		notifyMsg := notifyJsonMap(heartData)
		timestamp, _ := strconv.Atoi(notifyMsg["timestamp"])
		if tunix-int64(timestamp) <= timecheckInt64 || int64(timestamp)-tunix <= timecheckInt64 {
			addLog(0, "------->heart msg is:", string(heartData), time.Now().Unix())
			heartChan[notifyMsg["host"]] <- notifyMsg
		}
	}

	return nil
}
func heartCheck(serverip string, message chan map[string]string, checktime int64) {
	click := make(chan bool, 1)
	for {
		addHeartLog("Now click server:", serverip)
		go func() {
			time.Sleep(time.Duration(checktime) * time.Second)
			click <- true
		}()
		select {
		case msgData := <-message:
			if msgData["host"] == serverip {
				addHeartLog("The server ", serverip, " is alive,the msg is: ", msgData)
				if serverNotifyTime[serverip] != 0 {
					//	notifyRecover(serverip, serverNotifyTime[serverip])
				}
				<-click
			}
		case <-click:
			if serverNotifyTime[serverip] == 0 {
				serverNotifyTime[serverip] = int(time.Now().Unix())
			}
			addHeartLog("Error!!\tProxy server error timeout:", serverip)
			conn := initDbConn(-1)
			defer conn.Close()
			conn.Exec("START TRANSACTION")
			addHeartLog("START TRANSACTION")
			//update db
			sqlu := "update proxyServer set isAvailable = 0 where proxyServerIp = ? "
			stmt, _ := conn.Prepare(sqlu)
			if result, err := stmt.Exec(serverip); err == nil {
				if id, err := result.RowsAffected(); err == nil {
					addHeartLog("proxyServer is available : ", id, sqlu, serverip)
				} else {
					addHeartLog("proxyServer is available error: ", err, id, sqlu, serverip)
					conn.Exec("ROLLBACK")
					addHeartLog("ROLLBACK")
					return
				}
			} else {
				addHeartLog("proxyServer is available exec error: ", err, sqlu, serverip)
				conn.Exec("ROLLBACK")
				addHeartLog("ROLLBACK")
				return
			}
			conn.Exec("COMMIT")
			addHeartLog("COMMIT")
		}
	}
}
func initMq(logId int64) (*RabbitMQ, error) {
	mqHandle := new(RabbitMQ)
	mqHandle.Logn = logNormal
	mqHandle.Loge = logError
	err := mqHandle.Connect(config["amqp_uri"])
	if err != nil {
		addErrorLog(logId, "init mq Error!", err)
		return mqHandle, err
	}
	return mqHandle, nil
}
func deleteRuleByAppid(conn *sql.DB, data map[string]string, logId int64) error {
	appid := data["appid"]
	interport := data["port"]
	sql := `select distinct p.publicIp,proxyPort,portS,portM
			from appidProxyPort p inner join proxyServer s
			on p.publicIp = s.publicIp
			where proxyPort >= portS and proxyPort <= portM
			and appid='` + appid + `' and internalPort = ` + interport
	rows, err := conn.Query(sql)
	addLog(logId, "select delete rules:", sql)
	if err != nil {
		addErrorLog(logId, "sql delete rule error:", sql, err)
		return err
	}
	var pubIp, ports, portm string
	var openPort int
	hasrows := false
	for rows.Next() {
		if err := rows.Scan(&pubIp, &openPort, &ports, &portm); err == nil {
			hasrows = true
			addLog(logId, "get delete rule:", pubIp, ports, portm, openPort)
			err = agentClear(conn, pubIp, ports, portm, openPort, logId)
			if err != nil {
				addErrorLog(logId, "agent delete proxy Error:", appid, pubIp, ports, portm, openPort, "  ", err)
				return err
			}
		} else {
			addErrorLog(logId, "delete select error : ", err, appid, pubIp, ports, portm, openPort)
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
	sqld := "delete from port_proxy_rule where appid = ? and proxyPort = ?"
	stmt, _ := conn.Prepare(sqld)
	var deleteNum int
	if result, err := stmt.Exec(appid, openPort); err == nil {
		if num, err := result.RowsAffected(); err == nil {
			addLog(logId, "delete Rule By Appid and port delete result : ", num, sqld, appid, openPort)
			deleteNum = int(num)
		} else {
			addErrorLog(logId, "delete rule error : ", err, appid, openPort)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, "delete rule error : ", err, appid, openPort)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}

	sqld2 := "update proxyServer set rulesNumber = rulesNumber - ? where publicIp = ? and portS = ? and portM = ?"
	stmt, _ = conn.Prepare(sqld2)
	portsI, _ := strconv.Atoi(ports)
	portmI, _ := strconv.Atoi(portm)
	if result, err := stmt.Exec(deleteNum, pubIp, portsI, portmI); err == nil {
		if id, err := result.RowsAffected(); err == nil {
			addLog(logId, "addRule update affected number : ", id, deleteNum, pubIp, ports, portm)
		} else {
			addErrorLog(logId, "addRule update error : ", err, deleteNum, pubIp, ports, portm)
			conn.Exec("ROLLBACK")
			addLog(logId, "ROLLBACK")
			return err
		}
	} else {
		addErrorLog(logId, "addRule update error : ", err, deleteNum, pubIp, ports, portm)
		conn.Exec("ROLLBACK")
		addLog(logId, "ROLLBACK")
		return err
	}

	sqld3 := "update appidProxyPort set appid = ?, internalPort=? where publicIp = ? and proxyPort = ?"
	stmt, _ = conn.Prepare(sqld3)
	if result, err := stmt.Exec("0", 0, pubIp, openPort); err == nil {
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
	if deleteNum > 0 {
		addLog(logId, "recover availport :", pubIp, openPort)
		//portAvail[pubIp][openPort] = true
	}
	//end
	return err
}

func apolloScheduler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" && r.Method != "DELETE" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	questData, err := getQuestData(r, 0)
	if err != nil {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(0, "ListenAndServe: ", err)
		return
	}
	logid, isexist := questData["logid"]
	if isexist != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(0, "logid is not exist!: ", err)
		return
	}
	logidint, _ := strconv.Atoi(logid)
	logId := int64(logidint)
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)

	appid, e1 := questData["appid"]
	uiips, e2 := questData["ips"]
	if e1 != true || e2 != true {
		http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
		addErrorLog(logId, "param is lost!: ", e1, e2)
		return
	}
	addLog(logId, "quest data is:", questData)
	uiip_arr := strings.Split(uiips, ",")
	dbconn := initDbConn(logId)
	if r.Method == "POST" {
		if len(uiip_arr) == 1 {
			proto, pexist := questData["proto"]
			if pexist != true || (proto != "tcp" && proto != "udp") {
				http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
				addErrorLog(logId, "get param error!")
				return
			}
			interPort, intexist := questData["internalPort"]
			if intexist != true {
				http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
				addErrorLog(logId, "get param error!")
				return
			}

			var publicIp, portS, portM, openPort string
			//interPortInt, _ := strconv.Atoi(interPort)
			sqlc := "select count(*) from appidProxyPort where appid='" + appid + "' and internalPort=" + interPort
			rows, errc := dbconn.Query(sqlc)
			if errc != nil {
				http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
				addErrorLog(logId, "check appid if exist error:", errc, sqlc)
				return
			}
			var appnum int
			for rows.Next() {
				if err := rows.Scan(&appnum); err == nil {
					break
				}
			}
			if appnum <= 0 {
				http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
				addErrorLog(logId, "can not find the appid with the internal port!", sqlc)
				return
			} else {
				sql := "select distinct s.publicIp,proxyPort,portS,portM from appidProxyPort p inner join proxyServer s where p.proxyPort >= s.portS and p.proxyPort <=s.portM and appid='" + appid + "' and internalPort=" + interPort
				rows, err := dbconn.Query(sql)
				if err != nil {
					http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
					addErrorLog(logId, "get select port error:", err, sql)
					return
				}
				addLog(logId, "get rule from db ,get sql:", sql)
				var openportInt int
				for rows.Next() {
					if err = rows.Scan(&publicIp, &openportInt, &portS, &portM); err == nil {
						openPort = strconv.Itoa(openportInt)
						break
					} else {
						http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
						addErrorLog(logId, "get rule from db error:", err)
						return
					}
				}
			}
			go func() {
				defer dbconn.Close()

				cerr := createSingleRule(dbconn, appid, publicIp, portS, portM, openPort, uiips, proto, logId)
				if cerr != nil {
					//http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
					addErrorLog(logId, "createSingleRuleFromSche: ", cerr)
					return
				}
			}()

		} else {
			http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"result":"1"}`))
	} else if r.Method == "DELETE" {
		if len(uiip_arr) == 1 {
			go func() {
				defer dbconn.Close()
				derr := deleteRuleByChange(dbconn, appid, uiips, logId)
				if derr != nil {
					//http.Error(w, `{"result":"0"}`, http.StatusInternalServerError)
					addErrorLog(logId, "deleteRuleFromSche: ", derr)
					return
				}
			}()
		} else {
			http.Error(w, `{"result":"0"}`, http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"result":"1"}`))
	}
	addLog(logId, r.Host, r.RemoteAddr, r.Method, r.RequestURI, r.TLS, r.Trailer, r.URL)
}
*/
