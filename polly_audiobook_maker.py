import glob, os
import boto3

def cut_essay(infname, chunk_size=2500):
    import re

    # Set the maximum chunk size
    max_chunk_size = chunk_size

    # Read the essay
    with open(infname, 'r') as f:
        essay=f.read()

    # Split the essay into a list of words
    words = essay.split()

    # Initialize a list to store the chunks
    chunks = []
    chunk=[]
    # Initialize a counter to keep track of the chunk size
    chunk_size = 0

    # Iterate over the words in the essay
    for word in words:
    # Add the length of the current word to the chunk size
        chunk_size += len(word)
#    print(chunk_size, max_chunk_size)
    # If the chunk size is less than or equal to the maximum chunk size, add the word to the current chunk
        if chunk_size <= max_chunk_size:
            chunk.append(word)
    # If the chunk size is greater than the maximum chunk size, start a new chunk and add the word to it
        else:
            chunks.append(chunk)
            chunk = [word]
            chunk_size = len(word)

    # Add the final chunk to the list of chunks
    chunks.append(chunk)

    # Concatenate the chunks into strings
    chunks = [ " ".join(chunk) for chunk in chunks ]

    # Print the chunks
    #for chunk in chunks:
    #    print(chunk)
    return chunks


def gen_audiobook_mp3(text, polly, s3, mp3ofname, path='audiobooks', bucket='jls3b1'):
    # Convert the text to an audiobook using Amazon Polly
    response = polly.synthesize_speech(Text=text[:2980], OutputFormat='mp3', VoiceId='Joanna')

    # Save the audiobook to S3
    key=f'{path}/{mp3ofname}'    
    s3.put_object(Bucket=bucket, Key=key, Body=response['AudioStream'].read())

    # Download the audiobook from S3

    resp=s3.download_file(bucket, key, mp3ofname)

    print(f"Audiobook generated and downloaded successfully {bucket}/{path}")
    return key

def chunk_and_gen_audiobook(infname, polly, s3, output_dir):
    import time
    chunks=cut_essay(infname, chunk_size=2380)
    i=0
    nameprefix=os.path.basename(infname[:-4])    
    for chunk in chunks:
        i=i+1
        text=chunk

        mp3name=f'{output_dir}/{nameprefix}.%03d.mp3' % (i)

        # generating audio book and save 
        print(f'{len(text), polly, s3, mp3name}')
        gen_audiobook_mp3(text, polly, s3, mp3name)
        time.sleep(10)
        
def init_polly_s3():
    from datalib.cfgUtil import cfgUtil
    aws_key_dict=cfgUtil.get_aws_keys_from_ini()
    AWS_ACCESS_KEY = aws_key_dict['access_key']
    AWS_SECRET_KEY = aws_key_dict['secret_key']

    # Authenticate the session using your AWS access key and secret key
    polly = boto3.client('polly', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,region_name='us-east-1')
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    return polly, s3        

def batch_generate_audiobook(in_dirname='txt', output_dir='audiobooks'):
    import time
    import glob, os
    flist=glob.glob(f'{in_dirname}/*.txt')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    polly, s3=init_polly_s3()
    for infname in flist:
        print(f'converting fname:{infname}')
        chunk_and_gen_audiobook(infname, polly, s3, output_dir)    
        time.sleep(5)

if __name__=='__main__':
    batch_generate_audiobook('txt', 'audiobooks')
