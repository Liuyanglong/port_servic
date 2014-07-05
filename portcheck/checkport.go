package portcheck

import (
	"fmt"
	"net"
	"strings"
	"time"
)

//ip_port : e.g 111.222.333.444:12345
func CheckPortAvail(ip_port string, debug int) (bool, error) {
	conn, err := net.Dial("tcp4", ip_port)
	if err != nil {
		return false, err
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	defer conn.Close()
	var readbuf [10]byte
	var is_avail bool = false
	_, error := conn.Read(readbuf[0:])
	if error != nil {
		is_avail = !strings.Contains(error.Error(), "reset")
	}
	if debug == 1 {
		fmt.Println("port check avail:", ip_port, is_avail, error)
	}
	return is_avail, error
}
