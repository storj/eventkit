//go:build !unix

package listener

import "syscall"

var signalPrintStack = syscall.SIGINT
