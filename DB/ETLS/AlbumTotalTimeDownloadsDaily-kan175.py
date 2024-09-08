# handle incrementally
# code in comment works properly
# try:
#     existing_data = pd.read_sql(f'select * from {etl_table_name}', con = conn)
    
# except:
#     pass

# try:
#     existing_data.set_index('AlbumId', inplace=True)
#     final_data.set_index('AlbumId', inplace=True)
    
#     updated_data = final_data.combine_first(existing_data)
    
#     updated_data.reset_index(inplace=True)
    
#     updated_data['created_at'] = updated_data.apply(
#         lambda row: existing_data.loc[row['AlbumId'], 'created_at'] 
#         if row['AlbumId'] in existing_data.index 
#         else row['created_at'], axis=1
#     )
    
#     updated_data['updated_at'] = updated_data.apply(
#         lambda row: existing_data.loc[row['AlbumId'], 'updated_at']
#         if row['total_album_length'] == existing_data.loc[row['AlbumId'], 'total_album_length'] \
#             and row['total_album_downloads'] == existing_data.loc[row['AlbumId'], 'total_album_downloads']
#         else row['updated_at'], axis = 1
#     )
    
#     updated_data['updated_by'] = updated_data.apply(
#         lambda row: existing_data.loc[row['AlbumId'], 'updated_by']
#         if row['total_album_length'] == existing_data.loc[row['AlbumId'], 'total_album_length'] \
#             and row['total_album_downloads'] == existing_data.loc[row['AlbumId'], 'total_album_downloads']
#         else row['updated_by'], axis = 1
#     )

# except:
#     final_data = updated_data