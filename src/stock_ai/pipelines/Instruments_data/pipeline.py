from kedro.pipeline import Pipeline, node
from .nodes import download_file, unzip_file, append_date_to_filename

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=download_file,
                inputs=dict(url="params:complete_url", output_dir="params:raw_dir"),
                outputs="raw_complete_path",
                name="download_complete_file"
            ),
            node(
                func=unzip_file,
                inputs=dict(input_path="raw_complete_path", output_dir="params:intermediate_dir"),
                outputs="complete_unzipped",
                name="unzip_complete_file"
            ),
            node(
                func=append_date_to_filename,
                inputs="complete_unzipped",
                outputs=None,
                name="rename_complete_file"
            ),
            node(
                func=download_file,
                inputs=dict(url="params:nse_url", output_dir="params:raw_dir"),
                outputs="raw_nse_path",
                name="download_nse_file"
            ),
            node(
                func=unzip_file,
                inputs=dict(input_path="raw_nse_path", output_dir="params:intermediate_dir"),
                outputs="nse_unzipped",
                name="unzip_nse_file"
            ),
            node(
                func=append_date_to_filename,
                inputs="nse_unzipped",
                outputs=None,
                name="rename_nse_file"
            ),
            node(
                func=download_file,
                inputs=dict(url="params:bse_url", output_dir="params:raw_dir"),
                outputs="raw_bse_path",
                name="download_bse_file"
            ),
            node(
                func=unzip_file,
                inputs=dict(input_path="raw_bse_path", output_dir="params:intermediate_dir"),
                outputs="bse_unzipped",
                name="unzip_bse_file"
            ),
            node(
                func=append_date_to_filename,
                inputs="bse_unzipped",
                outputs=None,
                name="rename_bse_file"
            ),
            node(
                func=download_file,
                inputs=dict(url="params:mcx_url", output_dir="params:raw_dir"),
                outputs="raw_mcx_path",
                name="download_mcx_file"
            ),
            node(
                func=unzip_file,
                inputs=dict(input_path="raw_mcx_path", output_dir="params:intermediate_dir"),
                outputs="mcx_unzipped",
                name="unzip_mcx_file"
            ),
            node(
                func=append_date_to_filename,
                inputs="mcx_unzipped",
                outputs=None,
                name="rename_mcx_file"
            )
        ]
    )
