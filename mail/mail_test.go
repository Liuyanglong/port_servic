package mail

import (
	"testing"
)

func Test_sendmail(t *testing.T) {
	user := "liuyanglong@baidu.com"
	password := ""
	host := "mail2-in.baidu.com:25"
	to := "675044356@qq.com;liuyanglong@baidu.com"

	subject := "Test send email by golang"

	body := `
	<html>
	<body>
	<h3>
	"hello liuyanglong,how are you~~~"
	</h3>
	</body>
	</html>
	`
	err := SendMail(user, password, host, to, subject, body, "html")
	if err != nil {
		t.Error("send mail error", err)
	}

}
