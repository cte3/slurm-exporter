// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net/http"
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
}

func NewSlurmClient(server string, nodeFreq time.Duration, jobFreq time.Duration) (*SlurmClient, error) {
	token, ok := os.LookupEnv("SLURM_JWT")
	if !ok || token == "" {
		return nil, errors.New("SLURM_JWT must be defined and not empty")
	}

	if nodeFreq <= 1*time.Second || jobFreq <= 1*time.Second {
		return nil, errors.New("cache frequencies must be >= 1s")
	}

	customHTTPClient := &http.Client{
		Timeout: 5 * time.Minute,
	}

	config := &client.Config{
		Server:     server,
		AuthToken:  token,
		HTTPClient: customHTTPClient,
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
	}, nil
}

func (c *SlurmClient) SetHealthMetric(gauge prometheus.Gauge) {
	c.apiHealthy = gauge
}

func (c *SlurmClient) RunRefreshLoop(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("SlurmClient")
	go c.client.Start(ctx)

	ticker := time.NewTicker(c.nodeCacheFreq)
	defer ticker.Stop()

	for {
		err := c.client.List(ctx, &types.V0043NodeList{})
		if err != nil {
			logger.Error(err, "Slurm REST API is unreachable, serving cached metrics")
			if c.apiHealthy != nil {
				c.apiHealthy.Set(0)
			}
		} else {
			if c.apiHealthy != nil {
				c.apiHealthy.Set(1)
			}
			c.Lock()
			c.initialized = true
			c.Unlock()
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// --- client.Client Interface implementation ---
var jobQueryLock sync.Mutex

func (c *SlurmClient) List(ctx context.Context, list object.ObjectList, opts ...client.ListOption) error {
	// Check if we are dealing with the heavy Job list
	if _, ok := list.(*types.V0043JobInfoList); ok {
		if !jobQueryLock.TryLock() {
			return errors.New("job cache refresh already in progress; serving stale data")
		}
		defer jobQueryLock.Unlock()
	} else {
		c.Lock()
		defer c.Unlock()
	}

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

// FIX: Added Stop() to satisfy the final interface requirement
func (c *SlurmClient) Stop() {
	c.client.Stop()
}
