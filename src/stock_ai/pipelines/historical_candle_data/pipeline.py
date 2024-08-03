from kedro.pipeline import Pipeline, node
from datetime import datetime, timedelta
from stock_ai.pipelines.historical_candle_data.data_processing import preprocess_data
from stock_ai.pipelines.historical_candle_data.model_training import train_and_log_models

from .nodes import (
    load_access_token,
    fetch_historical_data,
    process_and_convert_to_parquet,
    calculate_indicators,
    feature_engineering,
    create_train_test_splits,
    create_train_test_splits_s3
    # process_and_save_parquet,
    # calculate_dates
)

def create_pipeline(**kwargs) -> Pipeline:
    # Define intervals
    # intervals = ['1minute', '30minute', 'day', 'week', 'month']
    # intervals = ['1minute', '30minute', 'day']
    intervals = ['day']
    intervals_w_m = ['week', 'month']
    # instrument_key = "NSE_INDEX|Nifty 50"
    # instrument_keys = ["NSE_INDEX|Nifty 50","NSE_INDEX|Nifty Bank"]
    instrument_keys = ["NSE_INDEX|Nifty Bank"]
    # TT_datasets = ["16w_8w","20w_4w","22w_2w","23w_1w"]
    TT_datasets = ["20w_4w"]
    
    
    # # Get the current date and time
    # end_date = datetime.now()

    # # Calculate the start date
    # start_date = end_date - timedelta(days=180)

    # # Format the dates as 'YYYY-MM-DD'
    # end_date = end_date.strftime('%Y-%m-%d')
    # start_date = start_date.strftime('%Y-%m-%d')
    
    # List to hold pipeline nodes
    nodes = []
    
    # Single node to load access token
    nodes.append(
        node(
            func=load_access_token,
            inputs="params:token_filepath",
            outputs="access_token",
            name="load_access_token_node"
        )
    )

    for instrument_key in instrument_keys:
        # Add nodes for each interval montly and weekly.. 
        
        for interval in intervals_w_m:            
                # Fetch historical data
                nodes.append(
                    node(
                        func=fetch_historical_data,
                        inputs=["access_token", f"params:instrument_key_{instrument_key.replace('|', '_').replace(' ', '_')}", f"params:interval_{interval}"],
                        outputs=f"raw_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                        name=f"fetch_historical_data_node_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                    )
                )            
                # pre process data and save csv fromat
                nodes.append(
                    node(
                        func=process_and_convert_to_parquet,
                        inputs=f"raw_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                        outputs=f"transformed_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",
                        name=f"process_and_save_parquet_node_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                        tags=["parallel"]
                    )
                )

        # Add nodes for each interval
        for interval in intervals:            
                # Fetch historical data
                # nodes.append(
                #     node(
                #         func=fetch_historical_data,
                #         inputs=["access_token", f"params:instrument_key_{instrument_key.replace('|', '_').replace(' ', '_')}", f"params:interval_{interval}"],
                #         outputs=f"raw_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                #         name=f"fetch_historical_data_node_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                #     )
                # )            
                # # pre process data and save csv fromat
                # nodes.append(
                #     node(
                #         func=process_and_convert_to_parquet,
                #         inputs=f"raw_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                #         outputs=f"transformed_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",
                #         name=f"process_and_save_parquet_node_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                #         tags=["parallel"]
                #     )
                # )
                # # Indicators data
                # nodes.append(
                #     node(
                #         func=calculate_indicators,
                #         inputs=f"transformed_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",
                #         outputs=f"indicators_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",
                #         name=f"indicators_node_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                #         tags=["parallel"]
                #     )
                # )
        
                # # feature_engineering data
                # nodes.append(
                # node(
                #     func=feature_engineering,
                #     inputs=f"indicators_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",
                #     outputs=f"feature_engineering_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",
                #     name=f"feature_engineering_node_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                #     tags=["parallel"]
                #     )
                # )
                for TT_dataset in TT_datasets: 
                    # nodes.append(
                    # node(
                    #     func=create_train_test_splits,
                    #     inputs=[f"feature_engineering_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@spark",f"params:{TT_dataset}"],
                    #     outputs=[f"train_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@spark"
                    #              ,f"test_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@spark"],
                    #     name=f"split_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}",
                    #     tags=["parallel"]
                    #     )
                    # )
                    # nodes.append(
                    # node(
                    #     func=create_train_test_splits_s3,
                    #     inputs=[f"feature_engineering_data_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}@csv",f"params:{TT_dataset}"],
                    #     outputs=[f"train_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3@csv"
                    #              ,f"test_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3@csv"],
                    #     name=f"split_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3",
                    #     tags=["parallel"]
                    #     )
                    # )

                    nodes.append(
                    node(
                        func=preprocess_data,
                        inputs= [f"train_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3@csv"
                                 ,f"test_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3@csv"],
                        outputs= [f"x_train_reshaped_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"y_train_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"x_test_reshaped_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"y_test_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"scaler_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"],
                        name=f"preprocess_data_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3",
                        tags=["parallel"]
                        )
                    )
                    nodes.append(
                    node(
                        func=train_and_log_models,
                        inputs= [f"x_train_reshaped_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"y_train_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"x_test_reshaped_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"y_test_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"],
                        outputs= [f"lstm_model_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"gru_model_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"lstm_history_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"
                                 ,f"gru_history_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}"],
                        name=f"train_and_log_models_{TT_dataset}_{instrument_key.replace('|', '_').replace(' ', '_')}_{interval}_s3",
                        tags=["parallel"]
                        )
                    )


    return Pipeline(nodes)
