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

from uuid import uuid4

from zenml.enums import ExecutionStatus
from zenml.zen_stores.dag_generator import DAGGeneratorHelper


def test_child_run_nodes_are_included_in_finalized_dag() -> None:
    """Tests that child run nodes are included in the finalized DAG."""
    helper = DAGGeneratorHelper()

    child_node = helper.add_child_run_node(
        node_id=helper.get_child_run_node_id("child-run"),
        id=uuid4(),
        name="child-run",
        status=ExecutionStatus.RUNNING.value,
    )
    helper.add_edge(source=child_node.node_id, target=child_node.node_id)

    dag = helper.finalize_dag(
        pipeline_run_id=uuid4(),
        status=ExecutionStatus.RUNNING,
    )

    assert any(node.type == "child_run" for node in dag.nodes)
    assert any(edge.source == child_node.node_id for edge in dag.edges)
