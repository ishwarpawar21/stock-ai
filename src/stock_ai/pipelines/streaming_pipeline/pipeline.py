from kedro.pipeline import Pipeline, node
from .nodes import (
    load_access_token,
    # validate_upstox_token,
    validate_websocket_data,
    validate_kinesis_stream,
    push_to_kinesis_from_websocket,

)

def create_pipeline(**kwargs) -> Pipeline:
    # Define instrument keys
    # instrument_keys = ["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"]
    instrument_keys = ["NSE_INDEX|Nifty 50"]

    # List to hold pipeline nodes
    nodes = []
    
    # Single node to load access token
    nodes.append(
        node(
            func=load_access_token,
            inputs="params:token_filepath",
            outputs="access_token",
            name="load_access_token_node"
        ))
    nodes.append(
        node(
            func=validate_kinesis_stream,
            inputs=["params:aws_access_key_id",
                    "params:aws_secret_access_key",
                    "params:region_name",
                    "params:kinesis_stream_name",
                    
                    ],
            outputs="access_kinesis",
            name="load_access_kinesis_node"
        ))
    # for instrument_key in instrument_keys:
    # # Node to fetch and validate WebSocket data
    #         nodes.append(
    #             node(
    #                 func=push_to_kinesis_from_websocket,
    #                 inputs=[
    #                     "access_token",
    #                     f"params:instrument_key_{instrument_key.replace('|', '_').replace(' ', '_')}",
    #                     "params:aws_access_key_id",
    #                     "params:aws_secret_access_key",
    #                     "params:region_name",
    #                     "params:kinesis_stream_name",
    #                 ],
    #                 outputs=f"validated_websocket_data_{instrument_key.replace('|', '_').replace(' ', '_')}",
    #                 name=f"push_to_kinesis_from_websocket_data_node_{instrument_key.replace('|', '_').replace(' ', '_')}"
    #             )
    #         )
    
    # return Pipeline(nodes)

    #   # Add nodes for each interval
    # for instrument_key in instrument_keys:
        # nodes.append(
        # node(
        #         func=validate_upstox_token,
        #         inputs=[
        #             "params:client_id",
        #             "params:client_secret",
        #             "params:auth_code",
        #             "params:redirect_uri",
        #             "params:grant_type"
        #         ],
        #         outputs="validated_access_token",
        #         name="validate_upstox_token_node"
        #     )
        # )

        # # Node to fetch and validate WebSocket data
        # nodes.append(
        #     node(
        #         func=fetch_and_validate_websocket_data,
        #         inputs=[
        #             "validated_access_token",
        #             "params:upstox_client_id",
        #             f"params:instrument_key_{instrument_key.replace('|', '_').replace(' ', '_')}"
        #         ],
        #         outputs=f"validated_websocket_data_{instrument_key.replace('|', '_').replace(' ', '_')}",
        #         name=f"fetch_and_validate_websocket_data_node_{instrument_key.replace('|', '_').replace(' ', '_')}"
        #     )
        # )

    # # Node to push validated data to AWS Kinesis
    # nodes.append(
    #     node(
    #         func=push_to_kinesis,
    #         inputs=[
    #             "validated_websocket_data",
    #             "params:kinesis_stream_name",
    #             "params:aws_access_key_id",
    #             "params:aws_secret_access_key",
    #             "params:region_name"
    #         ],
    #         outputs=None,
    #         name="push_to_kinesis_node"
    #     )
    # )

    return Pipeline(nodes)