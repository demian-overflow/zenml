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

import pytest

from zenml.execution.pipeline.dynamic.run_context import (
    DynamicPipelineRunContext,
)


def test_nested_dynamic_pipeline_run_context_instances_stack() -> None:
    """Tests that distinct nested dynamic contexts stack correctly."""
    outer = DynamicPipelineRunContext(
        pipeline=object(),  # type: ignore[arg-type]
        snapshot=object(),  # type: ignore[arg-type]
        run=object(),  # type: ignore[arg-type]
        runner=object(),  # type: ignore[arg-type]
    )
    inner = DynamicPipelineRunContext(
        pipeline=object(),  # type: ignore[arg-type]
        snapshot=object(),  # type: ignore[arg-type]
        run=object(),  # type: ignore[arg-type]
        runner=object(),  # type: ignore[arg-type]
    )

    assert DynamicPipelineRunContext.get() is None
    with outer:
        assert DynamicPipelineRunContext.get() is outer
        with inner:
            assert DynamicPipelineRunContext.get() is inner
        assert DynamicPipelineRunContext.get() is outer
    assert DynamicPipelineRunContext.get() is None


def test_dynamic_pipeline_run_context_reentry_of_same_instance_fails() -> None:
    """Tests that re-entering the same context instance fails."""
    context = DynamicPipelineRunContext(
        pipeline=object(),  # type: ignore[arg-type]
        snapshot=object(),  # type: ignore[arg-type]
        run=object(),  # type: ignore[arg-type]
        runner=object(),  # type: ignore[arg-type]
    )

    with context:
        with pytest.raises(RuntimeError):
            context.__enter__()
