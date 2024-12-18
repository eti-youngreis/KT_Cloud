def print_colored_line(text, color="reset"):
    colors = {
        "reset": "\033[0m", 
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "white": "\033[37m"
    }
    
    color_code = colors.get(color, colors["reset"]) 
    print(f"{color_code}{text}{colors['reset']}") 

### Execution Flow for S3 Storage Task Demonstration

# 1. **Step 1: Create a Bucket (Malki)**
#    - Create a **bucket directory** on the local filesystem.
#    - Set bucket attributes such as bucket name, creation date, region, etc.

# 2. **Step 2: Create an Object in the Bucket (Michal)**
#    - Inside the bucket directory, create an **object file** to store data.
#    - Save metadata (such as object key, size, and content type) in a **separate metadata file** (e.g., `metadata.json`).

# 3. **Step 3: Add Access Control List (ACL) to the Object (Shani K)**
#    - For the object, create an **ACL file** (in JSON or YAML format) that defines access permissions (e.g., read, write permissions).
#    - Similarly, create an ACL file for the bucket if needed.

# 4. **Step 4: Add Tags to the Object (Tamar G)**
#    - Create a **tags file** (e.g., `tags.json`) associated with the object.
#    - Add key-value pairs representing tags like “project:alpha” or “environment:test”.

# 5. **Step 5: Enable Object Versioning (Efrat A)**
#    - Enable **versioning** in the bucket.
#    - Create a new version of the object by storing the object with an updated filename (e.g., `object_v1`, `object_v2`).

# 6. **Step 6: Initiate Multipart Upload for a Large File (Shoshana L)**
#    - Create a **"multipart" sub-directory** inside the bucket.
#    - Store partial files temporarily during a multipart upload process. Once completed, merge the parts into the final object.

# 7. **Step 7: Apply Lifecycle Policy to the Bucket (Efrat H)**
#    - Create a **lifecycle policy file** (e.g., `lifecycle.json`) inside the bucket.
#    - Define rules for when to delete objects or move them to different storage classes based on age or usage.

# 8. **Step 8: Add Bucket Policy (Efrat R)**
#    - Create a **bucket policy file** (e.g., `policy.json`) inside the bucket directory.
#    - Define the bucket-level permissions, such as who can read, write, or delete objects in the bucket.

# 9. **Step 9: Lock the Object (Tamar M)**
#    - For specific objects, create a **lock file** or metadata that prevents modifications or deletions for a defined time period.

# 10. **Step 10: Set Up Event Notifications for the Bucket (Yehudit)**
#     - Create an **event notification configuration file** inside the bucket.
#     - Define triggers for actions like object creation, deletion, or modifications to notify external systems.

# 11. **Step 11: Encrypt the Object (Rachel)**
#     - Store an **encryption key file** or metadata alongside the object, defining how the object is encrypted.
#     - Ensure the object is saved in encrypted form, and the key is accessible for decryption when retrieving the object.

### Execution Order Summary:

# 1. **Create Bucket** → 
# 2. **Create Object in the Bucket** → 
# 3. **Add Access Control List (ACL)** → 
# 4. **Add Tags to the Object** → 
# 5. **Enable Versioning** → 
# 6. **Multipart Upload** (if needed for large files) → 
# 7. **Apply Lifecycle Policy** → 
# 8. **Add Bucket Policy** → 
# 9. **Lock the Object** → 
# 10. **Set Event Notifications** → 
# 11. **Encrypt the Object**


print'''---------------------Start Of session----------------------'''
print(current_date_time)

# object 1
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
print(total_duration)

print(current_date_time)
print'''---------------------End Of session----------------------'''
print(total_duration)
