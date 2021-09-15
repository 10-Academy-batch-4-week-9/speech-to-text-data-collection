def load_audio_s3(bucket_name: str, file_name: str):
    """ Load transcription data from s3 bucket"""
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1'
    )
    # Load file directly into python
    obj = s3.Bucket(bucket_name).Object(file_name).get()
    return obj['Body'].read()