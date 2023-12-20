//go:build !unix

package main

func GetChildrenUsage() (_ Usage, implemented bool, err error) {
	return Usage{}, false, nil
}
