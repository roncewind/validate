/*
Copyright © 2022  Ron Lynn <dad@lynntribe.net>

*/
package main

import (
	"log"

	"github.com/roncewind/validate/cmd"
)

func main() {
	log.SetFlags(log.Llongfile | log.Ldate | log.Lmicroseconds | log.LUTC)

	cmd.Execute()
}
