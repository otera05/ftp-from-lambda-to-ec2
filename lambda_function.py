import ftplib
from ftplib import FTP
from logging import getLogger, StreamHandler, DEBUG, Formatter
import boto3
import os
import sys


# ログ出力設定
logger = getLogger(__name__)
formatter = Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S')
handler = StreamHandler()
handler.setLevel(DEBUG)
handler.setFormatter(formatter)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False

# 環境変数設定
HOST = os.environ['HOST']
USER = os.environ['USER']
PASSWD = os.environ['PASSWD']
PATH = os.environ['PATH']

def upload_ftp_server(file_name, download_path):
    """
    VPC内のFTPサーバに接続してS3からダウンロードした
    ファイルをFTPサーバにアップロードするメソッド
    """
    # FTP接続
    logger.debug(HOST + ' にユーザ名：' + USER + ' でFTP接続を開始します。')
    try:
        ftp = FTP(HOST, USER, passwd=PASSWD)
        logger.debug('接続しました。ファイルのアップロードを開始します。')
        # カレントディレクトリの指定
        ftp.cwd(PATH)

        # 発行するFTPコマンド
        cmd = 'STOR ' + file_name

        # ファイルのアップロード
        with open(download_path + file_name, 'rb') as f:
            ftp.storbinary(cmd, f)

        logger.debug('ファイルのアップロードが完了しました。')

        # ダウンロードしたファイルの削除
        logger.debug('Lambda上にダウンロードしたファイルを削除します。')
        os.remove(download_path + file_name)
        logger.debug('Lambda上にダウンロードしたファイルの削除が完了しました。')

        return True

    except ftplib.all_errors as e:
        logger.error('FTPサーバとの接続時にエラーが発生しました： %s' % e)
        return False


def copy_to_backup_bucket(bucket_name, file_name, s3):
    """
    アップロード済みファイルをバックアップ用バケットに移動させるメソッド
    単純な「移動」ができないので「コピー」して元ファイルを「削除」している
    """
    # S3のオブジェクトをバックアップフォルダに移動
    copy_source = {
        'Bucket': bucket_name,
        'Key': file_name
    }
    backup_bucket_name = bucket_name + '-backup'
    backup_bucket = s3.Bucket(backup_bucket_name)
    obj = backup_bucket.Object(file_name)

    logger.debug('S3バケット「' + bucket_name + '」から「' + backup_bucket_name + '」へ「' + file_name + '」をコピーします。')
    obj.copy(copy_source)
    logger.debug('コピーが完了しました。')

    # コピー元ファイルを削除
    logger.debug('S3上のコピー元ファイルを削除します。')
    logger.debug('バケット名：' + bucket_name + ' ファイル名：' + file_name)
    s3.Object(bucket_name, file_name).delete()
    logger.debug('S3上のコピー元ファイルの削除が完了しました。')


def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    download_path = '/tmp/'

    # S3からファイルをダウンロード
    s3 = boto3.resource('s3')
    logger.debug('S3バケット「' + bucket_name + '」から「' + file_name + '」をダウンロードします。')
    s3.Object(bucket_name, file_name).download_file(download_path + file_name)
    logger.debug('ダウンロードに成功しました。')
    
    upload_flag = upload_ftp_server(file_name, download_path)

    if upload_flag:
        copy_to_backup_bucket(bucket_name, file_name, s3)
        
        return {
            'statusCode': 200,
            'body': '全ての処理が正常に完了しました。'
        }
    else:
        return {
            'statusCode': 200,
            'body': 'FTPサーバへのアップロードが正常に完了しませんでした。'
        }
