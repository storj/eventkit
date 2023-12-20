package main

import "time"

type Usage struct {
	UserTime   time.Duration
	SystemTime time.Duration
	MaxRSS     int64
}
