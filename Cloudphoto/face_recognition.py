import base64

import boto3
import configparser
import os
import json
import requests
import uuid

from PIL import Image

cfg = configparser.ConfigParser()
cfg.read('config.ini')

bucket = cfg['yandex']['bucket_name']
aws_key_id = cfg['AWS']['aws_access_key_id']
aws_secret_key = cfg['AWS']['aws_secret_access_key']
aws_region = cfg['AWS']['region']
yandex_endpoint_url = cfg['yandex']['endpoint']
yandex_api_key = cfg['yandex']['api_key']
yandex_queue_url = cfg['yandex']['queue_url']

SYSTEM_FACES_PREFIX = cfg['app']['SYSTEM_FACES_PREFIX']

session = boto3.session.Session()
s3client = session.client(service_name='s3',
                          region_name=aws_region,
                          endpoint_url=yandex_endpoint_url,
                          aws_access_key_id=aws_key_id,
                          aws_secret_access_key=aws_secret_key)
sqs = session.client(service_name='sqs',
                     endpoint_url='https://message-queue.api.cloud.yandex.net',
                     aws_access_key_id=aws_key_id,
                     aws_secret_access_key=aws_secret_key,
                     region_name=aws_region)


def handler(event, context):
    object_details = event['messages'][0]['details']
    bucket = object_details['bucket_id']
    key = object_details['object_id']

    filename = key.split('/')[-1]
    tmp_filename = f'/tmp/{filename}'

    s3client.download_file(bucket, key, tmp_filename)
    resp = recognize_faces(open(tmp_filename, 'rb'))['results'][0]['results'][0]['faceDetection']

    if 'faces' in resp:
        faces = resp['faces']
        print(f'{len(faces)} faces detected')

        faces_keys = []
        image = Image.open(tmp_filename)
        tmp_face_filename = '/tmp/face.jpg'
        for face in faces:
            face_cropped = image.crop((
                int(face['boundingBox']['vertices'][0]['x']),
                int(face['boundingBox']['vertices'][0]['y']),
                int(face['boundingBox']['vertices'][2]['x']),
                int(face['boundingBox']['vertices'][2]['y'])
            ))
            face_cropped.save(tmp_face_filename)
            _key = f'{SYSTEM_FACES_PREFIX}/face_{str(uuid.uuid4())}.jpg'
            s3client.upload_file(tmp_face_filename, bucket, _key, ExtraArgs={'Metadata': {'cropped_from': key}})
            faces_keys.append(_key)
        os.remove(tmp_face_filename)
        sqs.send_message(QueueUrl=yandex_queue_url, MessageBody=str(faces_keys))
    else:
        print(f'faces are not detected')

    os.remove(tmp_filename)

    return {
        'httpStatus': 200,
        'body': 'Success'
    }


def recognize_faces(file):
    data = json.dumps({
        "analyze_specs": [{
            "content": base64.b64encode(file.read()).decode('UTF-8'),
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    })
    request_headers = {
        'Authorization': f'Api-Key {yandex_api_key}',
        'Content-Type': 'application/json'
    }
    response = requests.post('https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze',
                             data=data,
                             headers=request_headers)
    return json.loads(response.text)
