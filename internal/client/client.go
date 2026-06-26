// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	"github.com/SlinkyProject/slurm-client/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type SlurmClient struct {
	sync.RWMutex
	client        client.Client
	apiHealthy    prometheus.Gauge
	server        string
	token         string
	nodeCacheFreq time.Duration
	jobCacheFreq  time.Duration
	initialized   bool

	jobMu    sync.RWMutex
	jobCache *types.V0043JobInfoList
	jobOnce  sync.Once
	jobReady chan struct{} // closed after the first successful job cache fill
}

func NewSlurmClient(server string, nodeFreq time.Duration, jobFreq time.Duration) (*SlurmClient, error) {
	token, ok := os.LookupEnv("SLURM_JWT")
	if !ok || token == "" {
		return nil, errors.New("SLURM_JWT must be defined and not empty")
	}

	if nodeFreq <= 1*time.Second || jobFreq <= 1*time.Second {
		return nil, errors.New("cache frequencies must be >= 1s")
	}

	config := &client.Config{
		Server:    server,
		AuthToken: token,
	}

	clientOptions := client.ClientOptions{
		EnableFor: []object.Object{
			&types.V0043JobInfo{},
			&types.V0043Node{},
			&types.V0043PartitionInfo{},
		},
		CacheSyncPeriod: nodeFreq,
	}

	innerClient, err := client.NewClient(config, &clientOptions)
	if err != nil {
		return nil, err
	}

	return &SlurmClient{
		client:        innerClient,
		server:        server,
		token:         token,
		nodeCacheFreq: nodeFreq,
		jobCacheFreq:  jobFreq,
		jobReady:      make(chan struct{}),
	}, nil
}

func (c *SlurmClient) SetHealthMetric(gauge prometheus.Gauge) {
	c.apiHealthy = gauge
}

func (c *SlurmClient) RunRefreshLoop(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("SlurmClient")
	go c.client.Start(ctx)
	go c.runJobRefreshLoop(ctx)

	backoff := time.Second

	for {
		err := c.client.List(ctx, &types.V0043NodeList{})
		if err != nil {
			logger.Error(err, "Slurm REST API is unreachable, serving cached metrics", "retry_in", backoff)
			if c.apiHealthy != nil {
				c.apiHealthy.Set(0)
			}
			delay := backoff
			backoff = min(backoff*2, c.nodeCacheFreq)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}

		if c.apiHealthy != nil {
			c.apiHealthy.Set(1)
		}
		c.Lock()
		c.initialized = true
		c.Unlock()
		backoff = time.Second

		select {
		case <-ctx.Done():
			return
		case <-time.After(c.nodeCacheFreq):
		}
	}
}

// runJobRefreshLoop proactively refreshes the job list cache at jobCacheFreq,
// backing off exponentially on API failures so collectors always read from
// a local copy instead of competing for a live API call.
func (c *SlurmClient) runJobRefreshLoop(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("SlurmClient")
	backoff := time.Second

	for {
		jobList := &types.V0043JobInfoList{}
		err := c.client.List(ctx, jobList)
		if err != nil {
			logger.Error(err, "Failed to refresh job cache, will retry", "retry_in", backoff)
			delay := backoff
			backoff = min(backoff*2, c.jobCacheFreq)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}

		c.jobMu.Lock()
		c.jobCache = jobList
		c.jobMu.Unlock()
		c.jobOnce.Do(func() { close(c.jobReady) })
		backoff = time.Second

		select {
		case <-ctx.Done():
			return
		case <-time.After(c.jobCacheFreq):
		}
	}
}

// --- client.Client Interface implementation ---

func (c *SlurmClient) List(ctx context.Context, list object.ObjectList, opts ...client.ListOption) error {
	if jobList, ok := list.(*types.V0043JobInfoList); ok {
		select {
		case <-c.jobReady:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.jobMu.RLock()
		*jobList = *c.jobCache
		c.jobMu.RUnlock()
		return nil
	}

	c.Lock()
	defer c.Unlock()
	return c.client.List(ctx, list, opts...)
}

func (c *SlurmClient) Get(ctx context.Context, key object.ObjectKey, obj object.Object, opts ...client.GetOption) error {
	return c.client.Get(ctx, key, obj, opts...)
}

func (c *SlurmClient) Create(ctx context.Context, obj object.Object, out any, opts ...client.CreateOption) error {
	return c.client.Create(ctx, obj, out, opts...)
}

func (c *SlurmClient) Update(ctx context.Context, obj object.Object, out any, opts ...client.UpdateOption) error {
	return c.client.Update(ctx, obj, out, opts...)
}

func (c *SlurmClient) Delete(ctx context.Context, obj object.Object, opts ...client.DeleteOption) error {
	return c.client.Delete(ctx, obj, opts...)
}

func (c *SlurmClient) GetInformer(obj object.ObjectType) client.InformerCache {
	return c.client.GetInformer(obj)
}

func (c *SlurmClient) GetServer() string {
	return c.server
}

func (c *SlurmClient) SetServer(server string) {
	c.Lock()
	defer c.Unlock()
	c.server = server
	c.client.SetServer(server)
}

func (c *SlurmClient) GetToken() string {
	return c.token
}

func (c *SlurmClient) SetToken(token string) {
	c.Lock()
	defer c.Unlock()
	c.token = token
	c.client.SetToken(token)
}

func (c *SlurmClient) Start(ctx context.Context) {
	// Managed via RunRefreshLoop
}

func (c *SlurmClient) Stop() {
	c.client.Stop()
}
