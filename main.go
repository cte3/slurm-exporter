// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/SlinkyProject/slurm-exporter/internal/client"
	"github.com/SlinkyProject/slurm-exporter/internal/collector"
)

var (
	setupLog = ctrl.Log.WithName("setup")
)

// Input flags to the command
type Flags struct {
	MetricsAddr   string
	Server        string
	NodeCacheFreq time.Duration
	JobCacheFreq  time.Duration
}

func parseFlags(flags *Flags) {
	flag.StringVar(
		&flags.MetricsAddr,
		"metrics-bind-address",
		":8080",
		"The address the metric endpoint binds to.",
	)
	flag.StringVar(
		&flags.Server,
		"server",
		"http://localhost:6820",
		"The server url of the cluster for the exporter to monitor.",
	)
	flag.DurationVar(
		&flags.NodeCacheFreq,
		"node-cache-freq",
		10*time.Second,
		"Sync frequency for Nodes and Partitions.",
	)
	flag.DurationVar(
		&flags.JobCacheFreq,
		"job-cache-freq",
		60*time.Second,
		"Sync frequency for Jobs (heavy request).",
	)
	flag.Parse()
}

func main() {
	var flags Flags
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	parseFlags(&flags)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog.Info("With", "Flags", flags)

	// Set up a context that responds to SIGINT/SIGTERM
	ctx := ctrl.SetupSignalHandler()

	slurmClient, err := client.NewSlurmClient(flags.Server, flags.NodeCacheFreq, flags.JobCacheFreq)
	if err != nil {
		setupLog.Error(err, "could not create slurm client")
		os.Exit(1)
	}

	// 2. Initialize and register the health metric.
	apiHealth := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slurm_exporter_api_healthy",
		Help: "1 if Slurm REST API is reachable and responding, 0 otherwise",
	})
	prometheus.MustRegister(apiHealth)
	slurmClient.SetHealthMetric(apiHealth)

	// 3. Start the refresh loop in the background.
	// This manages the underlying Slinky cache and updates apiHealth.
	go slurmClient.RunRefreshLoop(ctx)

	// 4. Initialize collectors.
	// They receive our slurmClient wrapper which implements the client.Client interface.
	collectors := []prometheus.Collector{
		collector.NewSchedulerCollector(slurmClient),
		collector.NewNodeCollector(slurmClient),
		collector.NewJobCollector(slurmClient),
		collector.NewPartitionCollector(slurmClient),
		collector.NewAccountCollector(slurmClient),
		collector.NewUserCollector(slurmClient),
	}
	for _, collector := range collectors {
		prometheus.MustRegister(collector)
	}

	setupLog.Info("starting exporter")
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(flags.MetricsAddr, nil); err != nil {
		setupLog.Error(err, "problem running exporter")
		os.Exit(1)
	}
}
