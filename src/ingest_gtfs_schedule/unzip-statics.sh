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
    #unzip -UUo "$zip_file" -d "../$destination_folder" &
    7za x -y $zip_file -o../$destination_folder
    echo "Unzipped"
done
wait

echo "Unzipping complete!"

#flatten each feed
cd ..

# Loop through each subdirectory
cargo run --bin flattenuncompressed

# we also need to change the permissions bug
# example is f-kauai 

chmod -R +r $destination_dir
