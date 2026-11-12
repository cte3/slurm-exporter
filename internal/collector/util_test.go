// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"encoding/json"
	"testing"

	api "github.com/SlinkyProject/slurm-client/api/v0043"
)

// Regression: slurmrestd sends uint32 flag values (e.g. CPU_FREQ_PERFORMANCE
// 0x82000000) in uint32_no_val fields; the upstream generated type was int32
// and failed to decode the entire job list. Requires the third_party patch.
func TestJobInfoDecodesUint32Governor(t *testing.T) {
	raw := `{"job_id":1,"cpu_frequency_governor":{"set":true,"infinite":false,"number":2181038080}}`
	var job api.V0043JobInfo
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got := ParseUint32NoVal(job.CpuFrequencyGovernor); got != 0x82000000 {
		t.Errorf("ParseUint32NoVal() = %#x, want %#x", got, 0x82000000)
	}
}
