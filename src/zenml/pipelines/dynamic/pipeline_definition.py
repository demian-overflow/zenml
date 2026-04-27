#  Copyright (c) ZenML GmbH 2025. All Rights Reserved.
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
"""Dynamic pipeline definition."""

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

from pydantic import BaseModel, ConfigDict, create_model

from zenml.client import Client
from zenml.config.source import Source
from zenml.enums import ExecutionMode
from zenml.execution.pipeline.utils import (
    should_prevent_pipeline_execution,
)
from zenml.logger import get_logger
from zenml.pipelines.pipeline_definition import Pipeline
from zenml.steps.utils import (
    parse_return_type_annotations,
)
from zenml.utils import source_utils

if TYPE_CHECKING:
    from zenml.execution.pipeline.dynamic.outputs import (
        AnyStepFuture,
        PipelineFuture,
    )
    from zenml.steps import BaseStep

logger = get_logger(__name__)


class DynamicPipeline(Pipeline):
    """Dynamic pipeline class."""

    def __init__(
        self,
        *,
        depends_on: Optional[List["BaseStep"]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the pipeline.

        Args:
            depends_on: The steps that the pipeline depends on.
            **kwargs: Pipeline constructor keyword arguments.
        """
        # This is the only execution mode that is currently supported for
        # dynamic pipelines, so we default to it.
        if kwargs.get("execution_mode", None) is None:
            kwargs["execution_mode"] = ExecutionMode.STOP_ON_FAILURE
        super().__init__(**kwargs)
        self._depends_on = depends_on or []
        self._validate_depends_on(self._depends_on)

    def _validate_depends_on(self, depends_on: List["BaseStep"]) -> None:
        """Validates the steps that the pipeline depends on.

        Args:
            depends_on: The steps that the pipeline depends on.

        Raises:
            RuntimeError: If some of the steps in `depends_on` are duplicated.
        """
        static_ids = set()
        for step in depends_on:
            static_id = step._static_id
            if static_id in static_ids:
                raise RuntimeError(
                    f"The pipeline {self.name} depends on the same step "
                    f"({step.name}) multiple times. To fix this, remove the "
                    "duplicate from the `depends_on` list. You can pass the "
                    "same step function with multiple configurations by using "
                    "the `step.with_options(...)` method."
                )

            static_ids.add(static_id)

    @property
    def depends_on(self) -> List["BaseStep"]:
        """The steps that the pipeline depends on.

        Returns:
            The steps that the pipeline depends on.
        """
        return self._depends_on

    @property
    def is_dynamic(self) -> bool:
        """If the pipeline is dynamic.

        Returns:
            If the pipeline is dynamic.
        """
        return True

    def resolve(self) -> "Source":
        """Resolves the pipeline.

        Raises:
            RuntimeError: If the resolved source is not loadable for dynamic
                pipelines.

        Returns:
            The pipeline source.
        """
        source = super().resolve()
        # We need to validate that the source is loadable for dynamic
        # pipelines as the orchestration environment will need to load the
        # source.
        try:
            source_utils.load(source)
        except Exception as e:
            raise RuntimeError(
                "Unable to resolve dynamic pipeline source. Make sure "
                "your pipeline is defined at the top level of your module."
            ) from e

        return source

    def _prepare_invocations(self, **kwargs: Any) -> None:
        """Prepares the invocations of the pipeline.

        Args:
            **kwargs: Keyword arguments.
        """
        for step in self._depends_on:
            self.add_step_invocation(
                step,
                input_artifacts={},
                external_artifacts={},
                model_artifacts_or_metadata={},
                client_lazy_loaders={},
                parameters={},
                default_parameters={},
                upstream_steps=set(),
            )

    def _submit_subpipeline(
        self,
        *args: Any,
        concurrent: bool,
        after: Union[
            "AnyStepFuture",
            Sequence["AnyStepFuture"],
            None,
        ] = None,
        **kwargs: Any,
    ) -> Union["PipelineFuture", Any]:
        """Submit this dynamic pipeline as a sub-pipeline.

        Args:
            *args: Entrypoint function arguments.
            concurrent: Whether to run the sub-pipeline concurrently.
            after: Optional dependency futures.
            **kwargs: Entrypoint function keyword arguments.

        Raises:
            RuntimeError: If called outside a dynamic run or inside a step
                body.

        Returns:
            A pipeline future for concurrent mode or resolved outputs for sync
            mode.
        """
        from zenml.execution.pipeline.dynamic.run_context import (
            DynamicPipelineRunContext,
        )
        from zenml.steps.step_context import StepContext

        run_context = DynamicPipelineRunContext.get()
        if not run_context:
            raise RuntimeError(
                "Submitting a pipeline is only possible within a dynamic "
                "pipeline run context."
            )

        if StepContext.get():
            raise RuntimeError(
                "Sub-pipeline calls are only allowed in a dynamic @pipeline "
                "body, not inside step bodies."
            )

        # TODO: Add support for PipelineFuture in `after=` dependencies.
        if after is None:
            after_list = None
        elif isinstance(after, (list, tuple)):
            after_list = after
        else:
            after_list = [after]
        return run_context.runner.submit_subpipeline(
            pipeline=self,
            args=args,
            kwargs=kwargs,
            concurrent=concurrent,
            after=cast(Optional[Sequence["AnyStepFuture"]], after_list),
        )

    def submit(
        self,
        *args: Any,
        after: Union[
            "AnyStepFuture",
            Sequence["AnyStepFuture"],
            None,
        ] = None,
        **kwargs: Any,
    ) -> "PipelineFuture":
        """Submit the pipeline to run concurrently as a sub-pipeline.

        Args:
            *args: Entrypoint function arguments.
            after: Optional dependency futures.
            **kwargs: Entrypoint function keyword arguments.

        Returns:
            The sub-pipeline future.
        """
        return cast(
            "PipelineFuture",
            self._submit_subpipeline(
                *args,
                concurrent=True,
                after=after,
                **kwargs,
            ),
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Run the pipeline on the active stack.

        Args:
            *args: Entrypoint function arguments.
            **kwargs: Entrypoint function keyword arguments.

        Raises:
            RuntimeError: If the active orchestrator does not support running
                dynamic pipelines.

        Returns:
            The sub-pipeline outputs when called from a dynamic run context,
            otherwise the top-level pipeline run or `None` if running with a
            schedule.
        """
        from zenml.execution.pipeline.dynamic.run_context import (
            DynamicPipelineRunContext,
        )

        if DynamicPipelineRunContext.get():
            return self._submit_subpipeline(
                *args,
                concurrent=False,
                **kwargs,
            )

        if should_prevent_pipeline_execution():
            logger.info("Preventing execution of pipeline '%s'.", self.name)
            return None

        stack = Client().active_stack
        if not stack.orchestrator.supports_dynamic_pipelines:
            raise RuntimeError(
                f"The {stack.orchestrator.__class__.__name__} does not "
                "support dynamic pipelines. "
            )

        self.prepare(*args, **kwargs)
        return self._run()

    def _compute_output_schema(self) -> Optional[Dict[str, Any]]:
        """Computes the output schema for the pipeline.

        Returns:
            The output schema for the pipeline.
        """
        try:
            outputs = parse_return_type_annotations(self.entrypoint)
            model_fields: Dict[str, Any] = {
                name: (output.resolved_annotation, ...)
                for name, output in outputs.items()
            }
            output_model: Type[BaseModel] = create_model(
                "PipelineOutput",
                __config__=ConfigDict(extra="forbid"),
                **model_fields,
            )
            return output_model.model_json_schema(mode="serialization")
        except Exception as e:
            logger.debug(
                f"Failed to generate the output schema for pipeline "
                f"`{self.name}: {e}. This means that the pipeline cannot be "
                "deployed.",
            )
            return None
