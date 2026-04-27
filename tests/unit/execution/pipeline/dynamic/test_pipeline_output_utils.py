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
"""Tests for dynamic pipeline output utility helpers."""

from uuid import uuid4

import pytest

from zenml.execution.pipeline.dynamic.outputs import OutputArtifact
from zenml.execution.pipeline.dynamic.pipeline_output_utils import (
    prepare_pipeline_output_update,
)


def _output_artifact(step_name: str, output_name: str) -> OutputArtifact:
    """Create a lightweight output artifact for utility tests.

    Args:
        step_name: Name of the step that produced the artifact.
        output_name: Output name associated with the artifact.

    Returns:
        An output artifact model instance.
    """
    artifact_id = uuid4()
    return OutputArtifact.model_construct(
        id=artifact_id,
        body=None,
        metadata=None,
        resources=None,
        name=f"{step_name}_{output_name}",
        output_name=output_name,
        step_name=step_name,
    )


def test_prepare_pipeline_output_update_single_output() -> None:
    """Tests output payload generation for a single artifact output."""
    artifact = _output_artifact(step_name="producer", output_name="artifact")

    def _entrypoint() -> OutputArtifact:
        raise AssertionError

    normalized, output_update = prepare_pipeline_output_update(
        value=artifact,
        pipeline_entrypoint=_entrypoint,
        valid_step_names={"producer"},
    )

    assert normalized is artifact
    assert output_update == {"output": artifact.id}


def test_prepare_pipeline_output_update_rejects_unknown_step() -> None:
    """Tests validation for artifacts produced by unrelated step runs."""
    artifact = _output_artifact(step_name="foreign", output_name="artifact")

    def _entrypoint() -> OutputArtifact:
        raise AssertionError

    with pytest.raises(RuntimeError, match="not part of this run"):
        prepare_pipeline_output_update(
            value=artifact,
            pipeline_entrypoint=_entrypoint,
            valid_step_names={"producer"},
        )
