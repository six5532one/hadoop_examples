from boto.s3.connection import S3Connection
from heapq import nlargest
from settings import AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY

def _file_content_iterator(filename):
    with open(filename, 'r') as f:
        for line in f:
            key, val = line.decode("utf-8").strip().split("\t") 
            yield key, int(val)


if __name__ == '__main__':
    conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
    input = "hurricanesandy-tweets"
    bucket = conn.get_bucket(input)
    with open("output_buffer", "a") as f:
        for key in bucket.list("output"):
            key.get_contents_to_file(f)
    top75 = nlargest(75, _file_content_iterator("output_buffer"), key=lambda x: x[1])
    for key, val in top75:
        print key, val
