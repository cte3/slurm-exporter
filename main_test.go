// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"testing"
	"time"

	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func Test_parseFlags(t *testing.T) {
	flags := Flags{}
	os.Args = []string{"test", "--metrics-bind-address", "8081", "--server", "foo", "--node-cache-freq", "10s", "--job-cache-freq", "60s"}
	parseFlags(&flags)
	if flags.MetricsAddr != "8081" {
		t.Errorf("Test_parseFlags() MetricsAddr = %v, want %v", flags.MetricsAddr, "8081")
	}
	if flags.Server != "foo" {
		t.Errorf("Test_parseFlags() Server = %v, want %v", flags.Server, "foo")
	}
	if flags.NodeCacheFreq != time.Second*10 {
		t.Errorf("Test_parseFlags() NodeCacheFreq = %v, want %v", flags.NodeCacheFreq, time.Second*10)
	}
	if flags.JobCacheFreq != time.Second*60 {
		t.Errorf("Test_parseFlags() JobCacheFreq = %v, want %v", flags.JobCacheFreq, time.Second*60)
	}
}
