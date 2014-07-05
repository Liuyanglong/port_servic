package main

import (
	"apollo/cfg"
	_ "apollo/mysql"
	"database/sql"
	"fmt"
)

func main() {
	cmap := make(map[string]string)
	err := cfg.Load("config.cfg", cmap)
	if err != nil {
		fmt.Println("Error:", err)
	}
	//dbHost := cmap["db_connect"]
	dbHost := cmap
	fmt.Println("db host is :", dbHost)
	dbconn, err := sql.Open("mysql", dbHost["db_connect"])
	if err != nil {
		fmt.Println("db error:", err)
	}

	//add msg
	stmt, err := dbconn.Prepare("insert into port_proxy_rule(ack,publicIp,proxyPort,uiIpPort) values (?,?,?,?)")
	if result, err := stmt.Exec("616616616", "10.10.10.10", "30000", "10.13.13.13:40000"); err == nil {
		if id, err := result.LastInsertId(); err == nil {
			fmt.Println("insert id : ", id)
		}
	}

	//get msg
	rows, err := dbconn.Query("select * from port_proxy_rule")
	if err != nil {
		fmt.Println("get Msg error:", err)
	}
	cols, _ := rows.Columns()
	for i := range cols {
		fmt.Print(cols[i])
		fmt.Print("\t")
	}

	var ack string
	var ip string
	var port string
	var remote string
	for rows.Next() {
		if err := rows.Scan(&ack, &ip, &port, &remote); err == nil {
			fmt.Print(ack)
			fmt.Print("\t")
			fmt.Print(ip)
			fmt.Print("\t")
			fmt.Print(port)
			fmt.Print("\t")
			fmt.Print(remote)
			fmt.Print("\t\r\n")
		}
	}

}
