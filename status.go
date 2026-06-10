// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"time"

	"github.com/hashicorp/raft"
)

// RaftStatus contains comprehensive operational status information about the Raft cluster.
type RaftStatus struct {
	// Cluster health
	Healthy       bool `json:"healthy"`
	HasLeader     bool `json:"has_leader"`
	LeaderHealthy bool `json:"leader_healthy"`

	// Local node information
	LocalID      string `json:"local_id"`
	LocalAddress string `json:"local_address"`
	State        string `json:"state"`
	IsLeader     bool   `json:"is_leader"`
	Suffrage     string `json:"suffrage"`

	// Leader information
	LeaderID      string `json:"leader_id"`
	LeaderAddress string `json:"leader_address"`

	// Index information - shows if nodes are in sync
	LastIndex   uint64 `json:"last_index"`   // Last log index
	LastApplied uint64 `json:"last_applied"` // Last applied to FSM
	CommitIndex uint64 `json:"commit_index"` // Last committed index
	LastContact string `json:"last_contact"` // Time since last leader contact

	// Cluster configuration
	Servers      []ServerStatus `json:"servers"`
	NumServers   int            `json:"num_servers"`
	NumVoters    int            `json:"num_voters"`
	NumNonvoters int            `json:"num_nonvoters"`

	// Additional metadata
	CacheName string            `json:"cache_name"`
	Stats     map[string]string `json:"stats,omitempty"` // Raw raft stats for debugging
}

// ServerStatus contains status information about a server in the Raft cluster.
type ServerStatus struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
	IsLeader bool   `json:"is_leader"`
	IsSelf   bool   `json:"is_self"`
}

// GetRaftStatus returns comprehensive operational status information about the Raft cluster.
// This includes cluster health, leader status, server list, and synchronization state.
func (s *Cache) GetRaftStatus() RaftStatus {
	cacheRaft := s.raft()
	status := RaftStatus{
		LocalID:      s.localID,
		LocalAddress: string(s.transport.LocalAddr()),
		State:        cacheRaft.State().String(),
		IsLeader:     s.IsLeader(),
		Suffrage:     suffrageToString(s.suffrage),
		CacheName:    s.name,
		LastIndex:    cacheRaft.LastIndex(),
		LastApplied:  cacheRaft.AppliedIndex(),
		CommitIndex:  cacheRaft.CommitIndex(),
		Stats:        cacheRaft.Stats(),
	}

	// Get leader information
	leaderAddr, leaderID := cacheRaft.LeaderWithID()
	status.LeaderID = string(leaderID)
	status.LeaderAddress = string(leaderAddr)
	status.HasLeader = leaderID != ""

	// Check if leader is healthy. VerifyLeader round-trips a quorum check
	// that only the leader itself can answer — followers and candidates fail
	// the verify future with ErrNotLeader (raft v1.7.3), which used to mark
	// every healthy follower's cluster unhealthy (RT-13042 H3). On non-leader
	// nodes, derive leader health from the recency of leader contact instead:
	// contact staler than HeartbeatTimeout is the same signal that makes a
	// follower start an election.
	if status.HasLeader {
		if status.IsLeader {
			verifyFuture := cacheRaft.VerifyLeader()
			status.LeaderHealthy = verifyFuture.Error() == nil
		} else {
			status.LeaderHealthy = time.Since(cacheRaft.LastContact()) <= s.heartbeatTimeout()
		}
	}

	// Get last contact time (only meaningful for followers)
	if !status.IsLeader {
		lastContact := cacheRaft.LastContact()
		status.LastContact = lastContact.String()
	} else {
		status.LastContact = "0s (leader)"
	}

	// Get server list and analyze cluster composition
	future := cacheRaft.GetConfiguration()
	if err := future.Error(); err == nil {
		configuration := future.Configuration()
		status.NumServers = len(configuration.Servers)

		for _, server := range configuration.Servers {
			serverStatus := ServerStatus{
				ID:       string(server.ID),
				Address:  string(server.Address),
				Suffrage: suffrageToString(server.Suffrage),
				IsLeader: server.ID == leaderID,
				IsSelf:   string(server.ID) == s.localID,
			}
			status.Servers = append(status.Servers, serverStatus)

			if server.Suffrage == raft.Voter {
				status.NumVoters++
			} else {
				status.NumNonvoters++
			}
		}
	}

	// Determine overall cluster health
	status.Healthy = s.determineClusterHealth(status)

	return status
}

// heartbeatTimeout returns the HeartbeatTimeout the raft instance runs with —
// the user-supplied raft config when set, raft's default otherwise. Read-only:
// deliberately does not go through raftConfig, which mutates the config.
func (s *Cache) heartbeatTimeout() time.Duration {
	if s.cfg.raftConfig != nil {
		return s.cfg.raftConfig.HeartbeatTimeout
	}
	return raft.DefaultConfig().HeartbeatTimeout
}

// determineClusterHealth evaluates the overall health of the Raft cluster.
func (s *Cache) determineClusterHealth(status RaftStatus) bool {
	// Must have a leader
	if !status.HasLeader {
		return false
	}

	// Leader must be healthy (reachable)
	if !status.LeaderHealthy {
		return false
	}

	// Must have at least one voter (should always be true if configured correctly)
	if status.NumVoters == 0 {
		return false
	}

	// Check if this node is significantly behind
	// If commit index is far behind last index, there might be issues
	if status.CommitIndex > 0 && status.LastApplied > 0 {
		lag := status.CommitIndex - status.LastApplied
		// Allow some lag, but if it's more than 1000 entries, something might be wrong
		if lag > 1000 {
			return false
		}
	}

	return true
}

// suffrageToString converts a raft.ServerSuffrage to a string representation.
func suffrageToString(suffrage raft.ServerSuffrage) string {
	switch suffrage {
	case raft.Voter:
		return "voter"
	case raft.Nonvoter:
		return "nonvoter"
	case raft.Staging:
		return "staging"
	default:
		return "unknown"
	}
}
