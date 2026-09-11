// Command siesta is the flink-siesta controller. This stub only reports its version; the
// controller wiring lands with the reconciler.
package main

import (
	"fmt"
	"os"
)

// version is set at build time: -ldflags "-X main.version=v0.1.0".
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	fmt.Fprintln(os.Stderr, "siesta: controller not wired yet, see docs/adr/")
	os.Exit(2)
}
