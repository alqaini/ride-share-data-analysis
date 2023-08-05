import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"])
    data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"]) 

    data.drop_duplicates().reset_index(drop=True)

    #set up the trip_id column as the index for the dataset
    data['trip_id'] = data.index  

    # partitioning the pickup_date_time into day, hour, month, year, weekday
    datetime_table = data[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_table['tpep_pickup_datetime'] = datetime_table['tpep_pickup_datetime']
    datetime_table['pick_hour'] = datetime_table['tpep_pickup_datetime'].dt.hour
    datetime_table['pick_day'] = datetime_table['tpep_pickup_datetime'].dt.day
    datetime_table['pick_month'] = datetime_table['tpep_pickup_datetime'].dt.month
    datetime_table['pick_year'] = datetime_table['tpep_pickup_datetime'].dt.year
    datetime_table['pick_weekday'] = datetime_table['tpep_pickup_datetime'].dt.weekday

    # partitioning the drop_off_date_time into day, hour, month, year, weekday
    datetime_table['tpep_dropoff_datetime'] = datetime_table['tpep_dropoff_datetime']
    datetime_table['drop_hour'] = datetime_table['tpep_dropoff_datetime'].dt.hour
    datetime_table['drop_day'] = datetime_table['tpep_dropoff_datetime'].dt.day
    datetime_table['drop_month'] = datetime_table['tpep_dropoff_datetime'].dt.month
    datetime_table['drop_year'] = datetime_table['tpep_dropoff_datetime'].dt.year
    datetime_table['drop_weekday'] = datetime_table['tpep_dropoff_datetime'].dt.weekday

    #creating an id for the datetime_table
    datetime_table['datetime_id'] = datetime_table.index

    # datetime_table = datetime_table.rename(columns={'tpep_pickup_datetime': 'datetime_id'}).reset_index(drop=True)
    datetime_table = datetime_table[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    #create a new table for the passenger count
    passenger_count_table = data[["passenger_count"]].reset_index(drop = True)

    #create an index for the passenger count
    passenger_count_table['passenger_count_id'] = passenger_count_table.index
    passenger_count_table = passenger_count_table[["passenger_count", "passenger_count_id"]]

    #get the rate code unique values to map it to the geographic location
    rate_code_data = data['RatecodeID'].unique()
    rate_code_data
    rate_code_names= {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_table = data[['RatecodeID']].reset_index(drop = True)
    rate_code_table['rate_code_id'] = rate_code_table.index
    rate_code_table['rate_code_name'] = rate_code_table['RatecodeID'].map(rate_code_names)
    rate_code_table = rate_code_table[['rate_code_id','RatecodeID','rate_code_name']]

    
    #create a pickup location table
    pickup_location_table = data[['pickup_longitude','pickup_latitude']].reset_index(drop = True)
    pickup_location_table['pickup_id'] = pickup_location_table.index
    pickup_location_table = pickup_location_table[['pickup_id','pickup_longitude','pickup_latitude']]

    #create dropoff location table
    dropoff_location_table = data[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_table['dropoff_id'] = dropoff_location_table.index
    dropoff_location_table = dropoff_location_table[['dropoff_id', 'dropoff_longitude','dropoff_latitude']]

    #create trip distance table
    distance_table = data[['trip_distance']].reset_index(drop=True)
    distance_table['trip_distance_id'] = distance_table.index
    distance_table = distance_table[['trip_distance_id','trip_distance']]

    #create trip payment table
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_table = data[['payment_type']].reset_index(drop=True)
    payment_table['payment_type_id'] = payment_table.index
    payment_table['payment_type_name'] = payment_table['payment_type'].map(payment_type_name)
    payment_table = payment_table[['payment_type_id', 'payment_type_name', 'payment_type']]

    #add the fact table and merge it with the other tables based on the id columns
    fact_table = data.merge(passenger_count_table, left_on='trip_id', right_on='passenger_count_id') \
                .merge(distance_table, left_on='trip_id', right_on='trip_distance_id') \
                .merge(rate_code_table, left_on='trip_id', right_on='rate_code_id') \
                .merge(pickup_location_table, left_on='trip_id', right_on='pickup_id') \
                .merge(dropoff_location_table, left_on='trip_id', right_on='dropoff_id')\
                .merge(datetime_table, left_on='trip_id', right_on='datetime_id') \
                .merge(payment_table, left_on='trip_id', right_on='payment_type_id') \
                [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_id', 'dropoff_id',
                'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount']]

    return {
        "passenger_count_table": passenger_count_table.to_dict(orient="dict"),
        "datetime_table": datetime_table.to_dict(orient="dict"),
        "distance_table": distance_table.to_dict(orient="dict"),
        "payment_table": payment_table.to_dict(orient="dict"),
        "rate_code_table": rate_code_table.to_dict(orient="dict"),
        "pickup_location_table": pickup_location_table.to_dict(orient="dict"),
        "dropoff_location_table": dropoff_location_table.to_dict(orient="dict"),
        "fact_table": fact_table.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'