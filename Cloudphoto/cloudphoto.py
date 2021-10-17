import boto3
import configparser
import argparse
from os import listdir
from os.path import join, isfile, isdir, exists


def run_upload(path, album):
    if path is None or album is None:
        raise AttributeError('Attributes \'path\' and \'album\' are required')
    validate_path(path)
    files = [f for f in listdir(path)
             if isfile(join(path, f)) and f.endswith(('.jpg', 'jpeg'))]
    for f in files:
        filename = f'{path}\\{f}'
        key = f'{album}/{f}'
        s3client.upload_file(filename, bucket, key)
        print(f'{filename} uploaded to Cloud')


def run_download(path, album):
    if path is None or album is None:
        raise AttributeError('Attributes \'path\' and \'album\' are required')

    validate_path(path)

    objects = s3client.list_objects_v2(Bucket=bucket, Prefix=f'{album}/')
    if objects['KeyCount'] == 0:
        print(f'There are no photos in \'{album}\' album')
    else:
        for file in objects['Contents']:
            filename_splitted = file['Key'].split('/')
            filename = path + '\\' + filename_splitted[len(filename_splitted) - 1]
            s3client.download_file(bucket, file['Key'], filename)
            print(f'File {filename} downloaded successfully')


def run_list(album=None):
    if album is None:
        common_prefixes = s3client.list_objects_v2(Bucket=bucket, Delimiter='/')['CommonPrefixes']
        albums = [f['Prefix'].split('/')[0] for f in common_prefixes]
        for album in albums:
            print(album)
    else:
        objects = s3client.list_objects_v2(Bucket=bucket, Prefix=f'{album}/')
        if objects['KeyCount'] > 0:
            for file in objects['Contents']:
                filename_splitted = file['Key'].split('/')
                print(filename_splitted[len(filename_splitted) - 1])


def validate_path(path):
    if not exists(path):
        raise FileNotFoundError(f'Path \'{path}\' not exists')
    if not isdir(path):
        raise NotADirectoryError(f'\'{path}\' is no a directory')


def configure_parser():
    parser = argparse.ArgumentParser()
    sub_parser = parser.add_subparsers(dest='command', required=True)

    upload_command = sub_parser.add_parser('upload', help='Uploading photos')
    download_command = sub_parser.add_parser('download', help='Downloading photos')
    list_command = sub_parser.add_parser('list')

    upload_command.add_argument('-p', '--path', type=str, dest='path', required=True)
    upload_command.add_argument('-a', '--album', type=str, dest='album', required=True)

    download_command.add_argument('-p', '--path', type=str, dest='path', required=True)
    download_command.add_argument('-a', '--album', type=str, dest='album', required=True)

    list_command.add_argument('-a', '--album', type=str, dest='album')
    return parser


if __name__ == '__main__':
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')

    bucket = cfg['yandex']['bucket_name']
    aws_key_id = cfg['AWS']['aws_access_key_id']
    aws_secret_key = cfg['AWS']['aws_secret_access_key']
    aws_region = cfg['AWS']['region']
    yandex_endpoint_url = cfg['yandex']['endpoint']

    arg_parser = configure_parser()
    args = arg_parser.parse_args()

    s3client = boto3.session.Session().client(service_name='s3',
                                              region_name=aws_region,
                                              endpoint_url=yandex_endpoint_url,
                                              aws_access_key_id=aws_key_id,
                                              aws_secret_access_key=aws_secret_key)

    if args.command == 'upload':
        run_upload(path=args.path, album=args.album)
    elif args.command == 'download':
        run_download(path=args.path, album=args.album)
    elif args.command == 'list':
        run_list(album=args.album)
