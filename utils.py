import pandas as pd

def load_data(output_data_dir_by_year,chunksize=1000000):
    '''
    This function loads data in chunks, writes outputs to text file by year
    output_data_dir_by_year : path to write out data by year
    chunksize: 
    '''




def get_date_timeseries(df,transactions_param, dates_param, customer_ids_param, frequency = 'month', ):
    '''
    This function accepts transactions and dates and customer ids 
    Args:
    df: pandas dataframe containing transactions, dates and customer ids
    transactions_param: columns name for transactions
    dates_param: columns name for dates already converted to datetime 
    customer_ids_param: columns name for customer_ids
    frequency: temporal aggregation of the data e.g. week, month
    Returns: 
    '''
    
    assert type(df[dates_param]) == ## check if pandas datetime / dayfirst etc


    df['month-year'] = df[dates_param].dt.to_period("M")  # returns the year-month
        
    if frequency == 'week':
        df['week'] = df[dates_param].dt.week  # returns the year-month
        grouped_df = df.groupby(subset=['month-year','week'])
    else:
        grouped_df = df.groupby(subset=['month-year'])

    for name, group in grouped_df:
         


    


    


def plot_date_timeseries(figname = 'date_timeseries.png'):
