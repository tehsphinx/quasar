// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
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
	status := RaftStatus{
		LocalID:      s.localID,
		LocalAddress: string(s.transport.LocalAddr()),
		State:        s.raft.State().String(),
		IsLeader:     s.IsLeader(),
		Suffrage:     suffrageToString(s.suffrage),
		CacheName:    s.name,
		LastIndex:    s.raft.LastIndex(),
		LastApplied:  s.raft.AppliedIndex(),
		CommitIndex:  s.raft.CommitIndex(),
		Stats:        s.raft.Stats(),
	}

	// Get leader information
	leaderAddr, leaderID := s.raft.LeaderWithID()
	status.LeaderID = string(leaderID)
	status.LeaderAddress = string(leaderAddr)
	status.HasLeader = leaderID != ""

	// Check if leader is healthy by verifying leadership
	if status.HasLeader {
		// VerifyLeader returns nil if this node believes the leader is still valid
		verifyFuture := s.raft.VerifyLeader()
		status.LeaderHealthy = verifyFuture.Error() == nil
	}

	// Get last contact time (only meaningful for followers)
	if !status.IsLeader {
		lastContact := s.raft.LastContact()
		status.LastContact = lastContact.String()
	} else {
		status.LastContact = "0s (leader)"
	}

	// Get server list and analyze cluster composition
	future := s.raft.GetConfiguration()
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
