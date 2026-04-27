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
"""Tests for dynamic future registry pipeline tracking."""

from concurrent.futures import Future

from zenml.execution.pipeline.dynamic.future_registry import (
    FutureRegistry,
    StartupCancelled,
)
from zenml.execution.pipeline.dynamic.outputs import PipelineFuture


def test_pipeline_future_registration_and_binding() -> None:
    """Tests pipeline future registration in the shared future registry."""
    registry = FutureRegistry()
    pipeline_future = PipelineFuture(output_names=["result"])
    pipeline_run_id = "child-run-id"
    wrapped: Future[object] = Future()

    registry.register_pipeline_future(
        pipeline_id=pipeline_run_id, future=pipeline_future
    )
    registry.bind_pipeline_execution_future(
        pipeline_id=pipeline_run_id,
        future=wrapped,
        cancel_handle=lambda exception: None,
    )

    assert registry.has_in_progress_work()
    wrapped.set_result(object())
    assert not registry.has_in_progress_work()


def test_pipeline_cancel_pending_work_uses_registered_handle() -> None:
    """Tests cancellation propagation via the pipeline future handle."""
    registry = FutureRegistry()
    pipeline_future = PipelineFuture(output_names=["result"])
    pipeline_run_id = "child-run-id"
    wrapped: Future[object] = Future()
    captured: list[str] = []

    registry.register_pipeline_future(
        pipeline_id=pipeline_run_id, future=pipeline_future
    )
    registry.bind_pipeline_execution_future(
        pipeline_id=pipeline_run_id,
        future=wrapped,
        cancel_handle=lambda exception: captured.append(str(exception)),
    )

    registry.cancel_pipeline_pending_work(
        pipeline_id=pipeline_run_id,
        exception=StartupCancelled("cancel child"),
    )

    assert captured == ["cancel child"]
