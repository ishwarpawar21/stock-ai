"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines


# from typing import Dict
# from kedro.pipeline import Pipeline
# from stock_ai.pipelines import streaming_pipeline

# def register_pipelines() -> Dict[str, Pipeline]:
#     return {
#         "streaming_pipeline": streaming_pipeline.create_pipeline(),
#     }