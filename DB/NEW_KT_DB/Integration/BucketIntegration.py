
# demonstrate all object functionallity

print'''---------------------Start Of session----------------------'''
print(current_date_time)

print('''{current_date_time} deonstration of object XXX start''')

# create
print('''{current_date_time} going to create db cluster names "example"''')
clusterController.create('example')
print('''{current_date_time} cluster "example" created successfully''')
print(total_duration)

# delete
print('''{current_date_time} going to delete db cluster "example"''')
clusterController.delete('example')
print('{current_date_time} verify db cluster "example" deleted by checking if it exist')
clusterTest.verify_deletion('example')
print('''{current_date_time} cluster "example" deleted successfully''')
print(total_duration)

print('''{current_date_time} deonstration of object XXX ended successfully''')
print(current_date_time)
print'''---------------------End Of session----------------------'''
print(total_duration)

