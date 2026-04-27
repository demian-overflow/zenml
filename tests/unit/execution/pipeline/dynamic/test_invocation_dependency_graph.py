#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for dynamic invocation dependency graph behavior."""

from zenml.execution.pipeline.dynamic.invocation_dependency_graph import (
    InvocationDependencyGraph,
    NodeState,
)


def test_subpipeline_node_registration_and_lookup() -> None:
    """Tests sub-pipeline node registration and typed lookup."""
    graph = InvocationDependencyGraph()

    nodes_ready = graph.register_subpipeline_node(node_id="pipeline:child")

    assert nodes_ready
    node = graph.get_subpipeline_node(node_id="pipeline:child")
    assert node.node_id == "pipeline:child"
    assert node.state == NodeState.READY


def test_ready_node_priority_prefers_step_then_subpipeline_then_map() -> None:
    """Tests ready node priority across step, sub-pipeline, and map nodes."""
    graph = InvocationDependencyGraph()

    graph.register_map_node(
        node_id="map-node",
        step=object(),  # type: ignore[arg-type]
        inputs={},
        product=False,
        state=NodeState.READY,
    )
    graph.register_subpipeline_node(
        node_id="pipeline-node",
        state=NodeState.READY,
    )
    graph.register_step_node(
        node_id="step-node",
        state=NodeState.READY,
    )

    assert graph.get_ready_node().node_id == "step-node"
    graph.mark_node_succeeded(node_id="step-node")
    assert graph.get_ready_node().node_id == "pipeline-node"
    graph.mark_node_succeeded(node_id="pipeline-node")
    assert graph.get_ready_node().node_id == "map-node"


def test_subpipeline_completion_unblocks_downstream_steps() -> None:
    """Tests downstream readiness after a sub-pipeline succeeds."""
    graph = InvocationDependencyGraph()

    graph.register_subpipeline_node(node_id="pipeline:child")
    graph.register_step_node(
        node_id="step:downstream",
        upstream_ids=["pipeline:child"],
    )

    assert (
        graph.get_step_node(node_id="step:downstream").state
        == NodeState.PENDING
    )
    graph.mark_node_succeeded(node_id="pipeline:child")
    assert (
        graph.get_step_node(node_id="step:downstream").state == NodeState.READY
    )
