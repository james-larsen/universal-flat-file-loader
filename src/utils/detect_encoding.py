import chardet

def detect_encoding(file_path):
    # Open the file in binary mode to prevent any decoding errors
    with open(file_path, 'rb') as f:
        # Read the first 10 rows of the file
        content = b''.join([f.readline() for _ in range(10)])
        # Determine the encoding of the content
        result = chardet.detect(content)

    return result['encoding']
