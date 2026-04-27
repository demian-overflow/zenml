"""Helpers for dynamic pipeline output validation and normalization."""

from typing import Any, Callable, Dict, List, Set, Tuple
from uuid import UUID

from zenml.execution.pipeline.dynamic.outputs import (
    OutputArtifact,
    StepRunOutputs,
)
from zenml.steps.utils import (
    SINGLE_RETURN_OUT_NAME,
    parse_return_type_annotations,
)


def validate_pipeline_output_value(
    value: Any, valid_step_names: Set[str]
) -> List[OutputArtifact]:
    """Validate and normalize a dynamic pipeline return value.

    Args:
        value: The dynamic pipeline return value.
        valid_step_names: Step invocation names that belong to this run.

    Raises:
        RuntimeError: If outputs are not valid output artifacts.

    Returns:
        A normalized list of output artifacts.
    """
    if value is None:
        return []

    if isinstance(value, OutputArtifact):
        artifacts = [value]
    elif isinstance(value, dict):
        raise RuntimeError(
            "Dynamic pipeline outputs must be returned as None, "
            "OutputArtifact or tuple[OutputArtifact, ...]. Dict returns "
            "are not supported."
        )
    elif isinstance(value, tuple):
        if not all(isinstance(item, OutputArtifact) for item in value):
            raise RuntimeError(
                "Pipeline outputs must be OutputArtifact instances "
                "produced by steps in this pipeline. Wrap computed values "
                "in a step."
            )
        artifacts = list(value)
    else:
        raise RuntimeError(
            "Pipeline outputs must be OutputArtifact instances produced by "
            "steps in this pipeline. Wrap computed values in a step."
        )

    for artifact in artifacts:
        if artifact.step_name not in valid_step_names:
            raise RuntimeError(
                f"Pipeline output `{artifact.output_name}` references step "
                f"`{artifact.step_name}` which is not part of this run."
            )
    return artifacts


def resolve_pipeline_output_names(
    pipeline_entrypoint: Callable[..., Any], count: int
) -> List[str]:
    """Resolve output names for persisted dynamic pipeline outputs.

    Args:
        pipeline_entrypoint: The dynamic pipeline entrypoint function.
        count: Number of output artifacts.

    Returns:
        Output names in deterministic order.
    """
    if count == 0:
        return []

    output_signatures = parse_return_type_annotations(pipeline_entrypoint)
    declared_names = list(output_signatures.keys())

    if count == 1:
        if declared_names:
            return [declared_names[0]]
        return [SINGLE_RETURN_OUT_NAME]

    if len(declared_names) == count:
        return declared_names

    return [f"output_{i}" for i in range(count)]


def prepare_pipeline_output_update(
    value: Any,
    pipeline_entrypoint: Callable[..., Any],
    valid_step_names: Set[str],
) -> Tuple[StepRunOutputs, Dict[str, UUID]]:
    """Prepare normalized outputs and payload for run status updates.

    Args:
        value: The dynamic pipeline return value.
        pipeline_entrypoint: Dynamic pipeline entrypoint for output naming.
        valid_step_names: Step invocation names that belong to this run.

    Returns:
        Normalized output artifact representation and output ID payload.
    """
    artifacts = validate_pipeline_output_value(
        value=value, valid_step_names=valid_step_names
    )
    output_names = resolve_pipeline_output_names(
        pipeline_entrypoint=pipeline_entrypoint, count=len(artifacts)
    )

    normalized_outputs: StepRunOutputs
    if not artifacts:
        normalized_outputs = None
    elif len(artifacts) == 1:
        normalized_outputs = artifacts[0]
    else:
        normalized_outputs = tuple(artifacts)

    outputs = {
        output_name: artifact.id
        for output_name, artifact in zip(output_names, artifacts)
    }
    return normalized_outputs, outputs
