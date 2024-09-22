
# demonstrate all object functionallity

print'''---------------------Start Of session----------------------'''print(current_date_time)

print('''{current_date_time} deonstration of object XXX start''')

# create
print('''{current_date_time} going to create bucket names "example"''')
bucketController.create('example')
print('''{current_date_time} bucket "example" created successfully''')
print(total_duration)

# delete
print('''{current_date_time} going to delete bucket "example"''')
bucketController.delete('example')
print('{current_date_time} verify bucket "example" deleted by checking if it exist')
bucketTest.verify_deletion('example')
print('''{current_date_time} bucket "example" deleted successfully''')
print(total_duration)

print('''{current_date_time} deonstration of object XXX ended successfully''')
print(current_date_time)
print('''---------------------End Of session----------------------''')
print'''---------------------End Of session----------------------'''