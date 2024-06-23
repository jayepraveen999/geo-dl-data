# Input and output file paths
input_file = 'copdem_tile.txt'
output_file = 'modified_copdem_tile.txt'

# Function to modify the lines
def modify_line(line):
    print(line)
    line = line.strip()  # Remove leading/trailing spaces or newlines
    parts = line.split('/')
    print(parts)  # Split the line by '/'
    filename = parts[-2]
    print(filename)  # Extract the filename
    modified_line = f"/vsis3/copernicus-dem-90m/{line}{filename}.tif\n"  # Modify the line
    return modified_line

# Read from input file and modify lines
with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
    for line in infile:
        modified = modify_line(line)
        outfile.write(modified)
          # Write the modified line to the output file

print(f"File '{output_file}' created with modified lines.")
