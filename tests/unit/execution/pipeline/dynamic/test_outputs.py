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
"""Tests for dynamic output future helpers."""

from concurrent.futures import Future

import pytest

from zenml.execution.pipeline.dynamic.outputs import PipelineFuture


def test_pipeline_future_exposes_outputs_by_name_and_index() -> None:
    """Tests PipelineFuture output access helpers."""
    wrapped: Future[tuple[object, object]] = Future()
    first = object()
    second = object()
    wrapped.set_result((first, second))

    future = PipelineFuture(output_names=["alpha", "beta"])
    future._set_startup_result(wrapped)

    assert future.get_artifact("alpha") is first
    assert future.get_artifact("beta") is second
    assert future[0] is first
    assert future[1] is second
    assert future[:] == (first, second)
    assert len(future) == 2
    assert tuple(future) == (first, second)
    assert future.was_awaited


def test_pipeline_future_handles_missing_output() -> None:
    """Tests missing output lookup on PipelineFuture."""
    wrapped: Future[tuple[object]] = Future()
    wrapped.set_result((object(),))

    future = PipelineFuture(output_names=["only"])
    future._set_startup_result(wrapped)

    with pytest.raises(KeyError):
        future.get_artifact("missing")


def test_pipeline_future_uses_cancel_handle_after_startup() -> None:
    """Tests that cancellation delegates to the future cancel handle."""
    wrapped: Future[tuple[object]] = Future()
    future = PipelineFuture(output_names=["only"])
    future._set_startup_result(wrapped)

    captured: list[str] = []

    def _cancel_handle(exception: BaseException) -> None:
        captured.append(str(exception))

    future._set_cancel_handle(_cancel_handle)
    future.cancel_pending_work(RuntimeError("cancel child"))

    assert captured == ["cancel child"]
