# handle incrementally
# code in comment doesn't work properly
# try: 
#     existing_data = pd.read_sql(f'select * from {etl_table_name}', con = conn)
# except:
#     pass
# try:
#     existing_data.set_index(['CustomerId', 'GenreId']).index.is_unique
#     final_data.set_index(['CustomerId', 'GenreId']).index.is_unique
    
#     updated_data = final_data.combine_first(existing_data)
    
#     updated_data.reset_index(inplace=True)
    
#     updated_data['created_at'] = updated_data.apply(
#         lambda row: existing_data.loc[row[['CustomerId', 'GenreId']], 'created_at'] 
#         if row[['CustomerId', 'GenreId']] in existing_data.index 
#         else row['created_at'], axis=1
#     )
    
#     updated_data['updated_at'] = updated_data.apply(
#         lambda row: existing_data.loc[row[['CustomerId', 'GenreId']], 'updated_at']
#         if row['revenue_overall'] == existing_data.loc[row[['CustomerId', 'GenreId']], 'revenue_overall']
#         else row['updated_at'], axis = 1
#     )
    
#     updated_data['updated_by'] = updated_data.apply(
#         lambda row: existing_data.loc[row[['CustomerId', 'GenreId']], 'updated_by']
#         if row['revenue_overall'] == existing_data.loc[row[['CustomerId', 'GenreId']], 'revenue_overall'] 
#         else row['updated_by'], axis = 1
#     )
    
#     updated_data = updated_data.toPandas()
#     print(updated_data)
    
# except:
#     final_data.reset_index(inplace=True)
#     final_data = updated_data
