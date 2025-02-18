/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "feature_barrier.h"

#include "cluster/feature_manager.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/sleep.hh>

namespace cluster {

ss::future<> feature_barrier_tag_state::wait_abortable(ss::abort_source& as) {
    if (!_exited) {
        as.check();
        auto sub_opt = as.subscribe([this]() noexcept { _exit_wait.broken(); });
        co_await _exit_wait.wait();
    }
}

ss::future<> feature_barrier_state::barrier(feature_barrier_tag tag) {
    vassert(
      ss::this_shard_id() == feature_manager::backend_shard,
      "Called barrier on wrong shard");

    vlog(clusterlog.debug, "barrier enter [{}] ({})", _self, tag);

    auto gate_holder = _gate.hold();

    if (!_members.contains(_self)) {
        vlog(
          clusterlog.debug,
          "waiting for cluster membership for barrier ({})",
          tag);
        co_await _members.await_membership(_self, _as);
    }

    if (_members.all_broker_ids().size() < 2) {
        // We are alone, immediate complete.
        vlog(clusterlog.debug, "barrier exit {} (single node)", tag);
        co_return;
    }

    // Set our entered state, so that any peers sending feature_barrier
    // requests will see it.
    update_barrier(tag, _self, true);

    // Announce to peers.  Iterate until we have successfully communicated
    // with all peers.
    std::set<model::node_id> sent_to;
    while (true) {
        bool all_sent = true;
        for (const auto& member_id : _members.all_broker_ids()) {
            if (member_id == _self) {
                // Don't try and send to self
                continue;
            }

            // Check early exit conditions before each RPC
            _as.check();

            // We only need to send an RPC to each peer once.  If they restart
            // during this time, they will do an RPC to us and get our readiness
            // in that way.
            if (sent_to.contains(member_id)) {
                vlog(
                  clusterlog.trace,
                  "barrier {} skipping peer {}, already communicated",
                  tag,
                  member_id);
                continue;
            }

            auto rpc_result = co_await _rpc_hook(_self, member_id, tag, true);

            if (rpc_result.has_error()) {
                // Throw abort_requested if the error occurred during shutdown
                _as.check();

                // Only at debug level because errors are totally expected
                // when cluster is in the middle of e.g. a rolling restart.
                auto& err = rpc_result.error();
                vlog(
                  clusterlog.debug,
                  "barrier exception sending to {}: {}",
                  member_id,
                  err);

                // Proceed to next node, and eventual retry of this node
                all_sent = false;
                continue;
            } else {
                auto result = rpc_result.value().data;

                sent_to.insert(member_id);

                if (result.complete) {
                    vlog(
                      clusterlog.debug,
                      "barrier {} (peer {} told us complete)",
                      tag,
                      member_id);
                    // Why don't we drop out when someone tells us they
                    // are complete?
                    // Because we must proceed around the loop until I have
                    // successfully communicated with all peers: this is
                    // necessary to ensure that they all know I am ready.
                }

                // Only apply this peer's `entered` if we didn't already
                // enter (prevent race between their RPC to us and our
                // RPC to them).
                if (!_barrier_state[tag].is_node_entered(member_id)) {
                    update_barrier(tag, member_id, result.entered);
                }
            }
        }

        if (all_sent) {
            break;
        } else {
            co_await ss::sleep_abortable(500ms, _as);
        }
    }
    auto& state = _barrier_state.at(tag);
    vlog(clusterlog.debug, "barrier tx complete, waiting ({})", tag);
    co_await state.wait_abortable(_as);
    vlog(clusterlog.debug, "barrier exit [{}] ({})", _self, tag);
}

/**
 * Call this when we get an RPC from another node that tells us
 * their barrier state.
 */
feature_barrier_response feature_barrier_state::update_barrier(
  feature_barrier_tag tag, model::node_id peer, bool entered) {
    vassert(
      ss::this_shard_id() == feature_manager::backend_shard,
      "Called barrier on wrong shard");
    vlog(
      clusterlog.trace,
      "update_barrier [{}] ({}, {}, {})",
      _self,
      tag,
      peer,
      entered);
    auto i = _barrier_state.find(tag);
    if (i == _barrier_state.end()) {
        _barrier_state[tag] = feature_barrier_tag_state({{peer, entered}});
        return {false, false};
    } else {
        i->second.node_enter(peer, entered);
        bool all_in = true;
        for (const auto& member_id : _members.all_broker_ids()) {
            if (!i->second.is_node_entered(member_id)) {
                vlog(
                  clusterlog.debug,
                  "update_barrier: not entered yet peer {} ({})",
                  member_id,
                  tag);
                all_in = false;
            }
        }

        if (all_in && !i->second.is_complete()) {
            vlog(clusterlog.debug, "barrier all in [{}] ({})", _self, tag);
            i->second.complete();
        }

        return {i->second.is_node_entered(_self), i->second.is_complete()};
    }
}

} // namespace cluster