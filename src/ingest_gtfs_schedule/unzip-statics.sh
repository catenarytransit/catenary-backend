#!/bin/bash

# Set the source and destination directories
source_dir="gtfs_static_zips/"
destination_dir="gtfs_uncompressed/"

# Change to the source directory
cd "$source_dir"

# Loop through each ZIP file in the source directory
for zip_file in *.zip; do
    # Create a folder with the same name as the ZIP file (without the .zip extension) in the destination directory
    folder_name="${zip_file%.zip}"
    destination_folder="$destination_dir/$folder_name"
    
    # Check if the destination folder already exists, if not, create it
    if [ ! -d "$destination_folder" ]; then
        mkdir -p "../$destination_folder"
    fi

    # Unzip the file into the destination folder
    unzip -f "$zip_file" -d "../$destination_folder"
    echo "Unzipped"
done

echo "Unzipping complete!"

# we also need to change the permissions bug
# example is f-kauai 

cd ..
cd "$destination_dir"

echo "finding and fixing read permissions"

subdirectories=$(find -type d)

# Loop through each subdirectory
for subdirectory in $subdirectories; do

  # Find all files in the subdirectory
  files=$(find $subdirectory -type f)

  # Loop through each file
  for file in $files; do

    # Check if the file has read permissions
    if [ ! -r $file ]; then

      # Add read permissions to the file
      
      echo "-----------------"
      echo "$subdirectory"
      echo "fixing missing read perms for $file"
      chmod +r $subdirectory/$file

    fi

  done

done