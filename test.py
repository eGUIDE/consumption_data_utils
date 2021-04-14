import pandas as pd
import smoothing as sm
import batch_smoothing as bsm




def test(raw,smoothed_df,consumer_id):
	smpo_raw=raw[raw.consumer_id==consumer_id].sort_values('transaction_date')
	smpo_raw=smpo_raw[~smpo_raw.tariff_name.isin(['TVA tax','Regulatory_Fee','Rura_fee'])]
	smpo_raw['dates']=pd.to_datetime(smpo_raw.transaction_date).dt.to_period('M')
	smpo_raw_sum=smpo_raw.groupby('dates').kWh_sold.sum().reset_index().sort_values('dates')

	smpo=smoothed_df[smoothed_df.consumer_id==consumer_id].sort_values(by=['year','month']) 
	smpo_sum=smpo.groupby(['month','year']).kWhs.sum().reset_index().sort_values(by=['year','month'])  
	return smpo_raw,smpo,smpo_raw_sum,smpo_sum


if __name__=='__main__':
	raw=pd.read_pickle('../../REG/REG_transaction_data.pck')

	sampo_100=pd.read_pickle('../../REG/sample_100_customers.pck')
	smoothed_df=pd.read_pickle('REG_smoothed_Feb_27_2021.pck')

	unique_ids=sampo_100.consumer_id.unique()
		
	consumer_id='100057'
	smpo_raw,smpo,smpo_raw_sum,smpo_sum=test(raw,smoothed_df,consumer_id)
	df,frst_day=sm.smoothing_func(smpo_raw,['transaction_date','kWh_sold'],3)
	consumer_id, month,year,consumption = bsm.get_monthly_profile(df,consumer_id,frst_day)
	mnthly=pd.DataFrame(data={'consumer_id':consumer_id,'month':month,'year':year,'kwh':consumption})
	mnthly.sort_values(by=['year','month'],inplace=True)
