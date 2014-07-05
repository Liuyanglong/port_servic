package main

import (
	"apollo/cfg"
	"fmt"
)

func main() {
	fmt.Println("start configFileReader...")
	cmap := make(map[string]string)
	err := cfg.Load("config.cfg", cmap)
	if err != nil {
		fmt.Println("Error:", err)
	}
	//fmt.Printf("%v\n", cmap)
	fmt.Println(cmap)
}
