# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with the Amazon Transcribe API to
transcribe an audio file to a text file. Also shows how to define a custom vocabulary
to improve the accuracy of the transcription.

This example uses a public domain audio file downloaded from Wikipedia and converted
from .ogg to .mp3 format. The file contains a reading of the poem Jabberwocky by
Lewis Carroll. The original audio source file can be found here:
    https://en.wikisource.org/wiki/File:Jabberwocky.ogg
"""

import logging
import sys
import os
import time
import boto3
from botocore.exceptions import ClientError
import requests
from pydub import AudioSegment
import csv
import moviepy.editor as mp
import json
import codecs
import datetime

# Add relative path to include demo_tools in this code example without need for setup.
sys.path.append('../..')
from demo_tools import CustomWaiter, WaitState

logger = logging.getLogger(__name__)


class TranscribeCompleteWaiter(CustomWaiter):
    """
    Waits for the transcription to complete.
    """
    def __init__(self, client):
        super().__init__(
            'TranscribeComplete', 'GetTranscriptionJob',
            'TranscriptionJob.TranscriptionJobStatus',
            {'COMPLETED': WaitState.SUCCESS, 'FAILED': WaitState.FAILURE},
            client)

    def wait(self, job_name):
        self._wait(TranscriptionJobName=job_name)


class VocabularyReadyWaiter(CustomWaiter):
    """
    Waits for the custom vocabulary to be ready for use.
    """
    def __init__(self, client):
        super().__init__(
            'VocabularyReady', 'GetVocabulary', 'VocabularyState',
            {'READY': WaitState.SUCCESS}, client)

    def wait(self, vocabulary_name):
        self._wait(VocabularyName=vocabulary_name)


def start_job(
        job_name, media_uri, media_format, language_code, transcribe_client,
        vocabulary_name=None, more_settings={}):
    """



    Starts a transcription job. This function returns as soon as the job is started.
    To get the current status of the job, call get_transcription_job. The job is
    successfully completed when the job status is 'COMPLETED'.

    :param more_settings: additional_settings added to jobargs bellow
    :param job_name: The name of the transcription job. This must be unique for
                     your AWS account.
    :param media_uri: The URI where the audio file is stored. This is typically
                      in an Amazon S3 bucket.
    :param media_format: The format of the audio file. For example, mp3 or wav.
    :param language_code: The language code of the audio file.
                          For example, en-US or ja-JP
    :param transcribe_client: The Boto3 Transcribe client.
    :param vocabulary_name: The name of a custom vocabulary to use when transcribing
                            the audio file.
    :return: Data about the job.
    """
    try:
        job_args = {
            'TranscriptionJobName': job_name,
            'Media': {'MediaFileUri': media_uri},
            'MediaFormat': media_format,
            'LanguageCode': language_code}
        if vocabulary_name is not None:
            job_args['Settings'] = {'VocabularyName': vocabulary_name}
        if job_args.get('Settings', None):
            job_args['Settings'].update(more_settings)
        else:
            if len(more_settings.items()) > 0:
                job_args['Settings'] = more_settings
        response = transcribe_client.start_transcription_job(**job_args)
        job = response['TranscriptionJob']
        logger.info("Started transcription job %s.", job_name)
    except ClientError:
        logger.exception("Couldn't start transcription job %s.", job_name)
        raise
    else:
        return job


def list_jobs(job_filter, transcribe_client):
    """
    Lists summaries of the transcription jobs for the current AWS account.

    :param job_filter: The list of returned jobs must contain this string in their
                       names.
    :param transcribe_client: The Boto3 Transcribe client.
    :return: The list of retrieved transcription job summaries.
    """
    try:
        response = transcribe_client.list_transcription_jobs(
            JobNameContains=job_filter)
        jobs = response['TranscriptionJobSummaries']
        next_token = response.get('NextToken')
        while next_token is not None:
            response = transcribe_client.list_transcription_jobs(
                JobNameContains=job_filter, NextToken=next_token)
            jobs += response['TranscriptionJobSummaries']
            next_token = response.get('NextToken')
        logger.info("Got %s jobs with filter %s.", len(jobs), job_filter)
    except ClientError:
        logger.exception("Couldn't get jobs with filter %s.", job_filter)
        raise
    else:
        return jobs


def get_job(job_name, transcribe_client):
    """
    Gets details about a transcription job.

    :param job_name: The name of the job to retrieve.
    :param transcribe_client: The Boto3 Transcribe client.
    :return: The retrieved transcription job.
    """
    try:
        response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name)
        job = response['TranscriptionJob']
        logger.info("Got job %s.", job['TranscriptionJobName'])
    except ClientError:
        logger.exception("Couldn't get job %s.", job_name)
        raise
    else:
        return job


def delete_job(job_name, transcribe_client):
    """
    Deletes a transcription job. This also deletes the transcript associated with
    the job.

    :param job_name: The name of the job to delete.
    :param transcribe_client: The Boto3 Transcribe client.
    """
    try:
        transcribe_client.delete_transcription_job(
            TranscriptionJobName=job_name)
        logger.info("Deleted job %s.", job_name)
    except ClientError:
        logger.exception("Couldn't delete job %s.", job_name)
        raise


def create_vocabulary(
        vocabulary_name, language_code, transcribe_client,
        phrases=None, table_uri=None):
    """
    Creates a custom vocabulary that can be used to improve the accuracy of
    transcription jobs. This function returns as soon as the vocabulary processing
    is started. Call get_vocabulary to get the current status of the vocabulary.
    The vocabulary is ready to use when its status is 'READY'.

    :param vocabulary_name: The name of the custom vocabulary.
    :param language_code: The language code of the vocabulary.
                          For example, en-US or nl-NL.
    :param transcribe_client: The Boto3 Transcribe client.
    :param phrases: A list of comma-separated phrases to include in the vocabulary.
    :param table_uri: A table of phrases and pronunciation hints to include in the
                      vocabulary.
    :return: Information about the newly created vocabulary.
    """
    try:
        vocab_args = {'VocabularyName': vocabulary_name, 'LanguageCode': language_code}
        if phrases is not None:
            vocab_args['Phrases'] = phrases
        elif table_uri is not None:
            vocab_args['VocabularyFileUri'] = table_uri
        response = transcribe_client.create_vocabulary(**vocab_args)
        logger.info("Created custom vocabulary %s.", response['VocabularyName'])
    except ClientError:
        logger.exception("Couldn't create custom vocabulary %s.", vocabulary_name)
        raise
    else:
        return response


def list_vocabularies(vocabulary_filter, transcribe_client):
    """
    Lists the custom vocabularies created for this AWS account.

    :param vocabulary_filter: The returned vocabularies must contain this string in
                              their names.
    :param transcribe_client: The Boto3 Transcribe client.
    :return: The list of retrieved vocabularies.
    """
    try:
        response = transcribe_client.list_vocabularies(
            NameContains=vocabulary_filter)
        vocabs = response['Vocabularies']
        next_token = response.get('NextToken')
        while next_token is not None:
            response = transcribe_client.list_vocabularies(
                NameContains=vocabulary_filter, NextToken=next_token)
            vocabs += response['Vocabularies']
            next_token = response.get('NextToken')
        logger.info(
            "Got %s vocabularies with filter %s.", len(vocabs), vocabulary_filter)
    except ClientError:
        logger.exception(
            "Couldn't list vocabularies with filter %s.", vocabulary_filter)
        raise
    else:
        return vocabs


def get_vocabulary(vocabulary_name, transcribe_client):
    """
    Gets information about a customer vocabulary.

    :param vocabulary_name: The name of the vocabulary to retrieve.
    :param transcribe_client: The Boto3 Transcribe client.
    :return: Information about the vocabulary.
    """
    try:
        response = transcribe_client.get_vocabulary(VocabularyName=vocabulary_name)
        logger.info("Got vocabulary %s.", response['VocabularyName'])
    except ClientError:
        logger.exception("Couldn't get vocabulary %s.", vocabulary_name)
        raise
    else:
        return response


def update_vocabulary(
        vocabulary_name, language_code, transcribe_client, phrases=None,
        table_uri=None):
    """
    Updates an existing custom vocabulary. The entire vocabulary is replaced with
    the contents of the update.

    :param vocabulary_name: The name of the vocabulary to update.
    :param language_code: The language code of the vocabulary.
    :param transcribe_client: The Boto3 Transcribe client.
    :param phrases: A list of comma-separated phrases to include in the vocabulary.
    :param table_uri: A table of phrases and pronunciation hints to include in the
                      vocabulary.
    """
    try:
        vocab_args = {'VocabularyName': vocabulary_name, 'LanguageCode': language_code}
        if phrases is not None:
            vocab_args['Phrases'] = phrases
        elif table_uri is not None:
            vocab_args['VocabularyFileUri'] = table_uri
        response = transcribe_client.update_vocabulary(**vocab_args)
        logger.info(
            "Updated custom vocabulary %s.", response['VocabularyName'])
    except ClientError:
        logger.exception("Couldn't update custom vocabulary %s.", vocabulary_name)
        raise


def delete_vocabulary(vocabulary_name, transcribe_client):
    """
    Deletes a custom vocabulary.

    :param vocabulary_name: The name of the vocabulary to delete.
    :param transcribe_client: The Boto3 Transcribe client.
    """
    try:
        transcribe_client.delete_vocabulary(VocabularyName=vocabulary_name)
        logger.info("Deleted vocabulary %s.", vocabulary_name)
    except ClientError:
        logger.exception("Couldn't delete vocabulary %s.", vocabulary_name)
        raise


def write_results_to_end(file_name, results):
    if not os.path.exists(file_name):
        # os.makedirs(results_path)
        with open(file_name, 'w') as fd_for_file:
            fd_for_file.write(results)
    else:
        with open(file_name, 'a') as fd_for_file:
            fd_for_file.write('\n')
            fd_for_file.write(results)
            fd_for_file.write('\n')


def replace_n(file_name):
    # home = os.path.expanduser('~')
    # results_path = os.path.join(home, 'Documents', 'transcriptions')
    if not os.path.exists(file_name):
        # os.makedirs(results_path)
        with open(file_name, 'r') as read_fd_for_file:
            t = read_fd_for_file.read()

    else:
        with open(file_name, 'r') as read_fd_for_file:
            t = read_fd_for_file.read()
            with open(file_name, 'w') as write_fd_for_file:
                write_fd_for_file.write(t.replace('/n', '\n'))


def aws_transcribe_loop_dir(path_to_dir, language_code):
    with open('rootkey.csv', newline='') as csvfile:
        config = {item[0]:item[1] for item in csv.reader(csvfile, delimiter='=')}
        config['language_code'] = language_code
    for path, subdirs, files in os.walk(path_to_dir):
        for name in files:
            try:
                short_name, extention = os.path.splitext(name)
                if extention == '.wav':
                    fullWav = path + '/' + name
                    fullMp3name = path + '/' + short_name + '.mp3'
                    AudioSegment.from_wav(fullWav).export(fullMp3name, format="mp3")
                    do_aws_transcription(file_name=fullMp3name, config=config )
                else :
                    if extention == '.mp3':
                        do_aws_transcription(path + '/' + name, config)
                if extention == '.txt':
                    replace_n(path + '/' + name)
            except Exception as e:
                print("error: {}".format(e))
                continue


def remove_buckets():
    csvfile = open('rootkey.csv', newline='')
    config = {item[0]: item[1] for item in csv.reader(csvfile, delimiter='=')}
    csvfile.close()
    s3 = boto3.client('s3', aws_access_key_id=config['AWSAccessKeyId'], aws_secret_access_key=config['AWSSecretKey'])
    buckets_dict = s3.list_buckets()
    buckets_list = buckets_dict.get('Buckets', None)

    s3resource = boto3.resource('s3',aws_access_key_id=config['AWSAccessKeyId'],
                                aws_secret_access_key=config['AWSSecretKey'])

    for item in buckets_list:
        try:
            if item['Name'] != 'zeev-bucket-1641252030744081000':
                delete_res = s3resource.Object(item['Name'], '20211207-C0015.mp3').delete()
                print(f"delete object response: {delete_res}")
                b = s3resource.Bucket(item['Name'])

                res = b.delete()
                print("delete bucket response: {}".format(res))
        except Exception as e:
            print("delete error : {}".format(e))


def convert_to_mp3(path_to_dir):
    for path, subdirs, files in os.walk(path_to_dir):
        for name in files:
            try:
                short_name, extension = os.path.splitext(name)
                if extension in ['.MOV', '.mov', '.MP4', '.mp4']:
                    full_movie_path = path + '/' + name
                    full_mp3_path = path + '/' + short_name + '.mp3'
                    clip = mp.VideoFileClip(full_movie_path)
                    clip.audio.write_audiofile(full_mp3_path)
            except Exception as err:
                print("error: {}".format(err))
                continue


def parse_transcription_results(file_name):
    name, extension = os.path.splitext(file_name)
    with codecs.open(name + '_parsed_' + '.txt', 'w', 'utf-8') as dest_file:
        with codecs.open(file_name, 'r', 'utf-8') as source_f:
            data = json.loads(source_f.read())
            labels = data['speaker_labels']['segments']
            speaker_start_times = {}
            for label in labels:
                for item in label['items']:
                    speaker_start_times[item['start_time']] = item['speaker_label']
            items = data['items']
            lines = []
            line = ''
            time = 0
            speaker = 'null'
            i = 0
            for item in items:
                i = i + 1
                content = item['alternatives'][0]['content']
                if item.get('start_time'):
                    current_speaker = speaker_start_times[item['start_time']]
                elif item['type'] == 'punctuation':
                    line = line + content
                if current_speaker != speaker:
                    if speaker:
                        lines.append({'speaker': speaker, 'line': line, 'time': time})
                    line = content
                    speaker = current_speaker
                    time = item['start_time']
                elif item['type'] != 'punctuation':
                    line = line + ' ' + content
            lines.append({'speaker': speaker, 'line': line, 'time': time})
            sorted_lines = sorted(lines, key=lambda k: float(k['time']))
            for line_data in sorted_lines:
                line = '[' + str(
                    datetime.timedelta(seconds=int(round(float(line_data['time']))))) + '] ' + line_data.get(
                    'speaker') + ': ' + line_data.get('line')
                dest_file.write(line + '\n\n')


def do_aws_transcription(file_name, config):
    """Shows how to use the Amazon Transcribe service."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    s3_resource = boto3.resource('s3',
                                 aws_access_key_id=config['AWSAccessKeyId'],
                                 aws_secret_access_key=config['AWSSecretKey']
                                 )
    transcribe_client = boto3.client('transcribe',
                                     aws_access_key_id=config['AWSAccessKeyId'],
                                     aws_secret_access_key=config['AWSSecretKey'],
                                     region_name='eu-west-1')

    bucket_name = f'zeev-bucket'
    print(f"Creating bucket {bucket_name}.")
    bucket = s3_resource.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': transcribe_client.meta.region_name})
    media_file_name = file_name
    media_object_key = f'{os.path.split(os.path.dirname(file_name))[1]}-{os.path.split(file_name)[1]}'

    pre, ext = os.path.splitext(media_file_name)
    result_file_name = pre + '.txt'
    print("result file name: " + result_file_name)

    print(f"Uploading media file {media_file_name}.")
    bucket.upload_file(media_file_name, media_object_key)
    media_uri = f's3://{bucket.name}/{media_object_key}'

    job_name_simple = f'Jabber-{time.time_ns()}'
    print(f"Starting transcription job {job_name_simple}.")
    start_job(
        job_name_simple, f's3://{bucket_name}/{media_object_key}', 'mp3', config['language_code'],
        transcribe_client, more_settings={'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 5})
    transcribe_waiter = TranscribeCompleteWaiter(transcribe_client)
    transcribe_waiter.wait(job_name_simple)
    job_simple = get_job(job_name_simple, transcribe_client)
    # transcription result
    transcript_simple = requests.get(
        job_simple['Transcript']['TranscriptFileUri']).json()
    print(f"Transcript for job {transcript_simple['jobName']}:")
    for item in transcript_simple['results']['transcripts']:
        print("transcript item: ".format(item))
    print('-'*88)
    results_json_path = os.path.splitext(result_file_name)[0] + '.json'
    with open(results_json_path, "w") as outfile:
        json.dump(transcript_simple.get('results', {"no": "results"}), outfile)
    for item in transcript_simple['results']['transcripts']:
        write_results_to_end(result_file_name, item['transcript'])

    # print("Creating a custom vocabulary that lists the nonsense words to try to "
    #       "improve the transcription.")
    # vocabulary_name = f'Jabber-vocabulary-{time.time_ns()}'
    # create_vocabulary(
    #     vocabulary_name, config['language_code'], transcribe_client,
    #     phrases=[
    #         'יהודים','עברית','שפה', 'דקות','רוסיה','לצאת','עלייה',],
    #     )


    # vocabulary_ready_waiter = VocabularyReadyWaiter(transcribe_client)
    # vocabulary_ready_waiter.wait(vocabulary_name)
    #
    # job_name_vocabulary_list = f'Jabber-vocabulary-list-{time.time_ns()}'
    # print(f"Starting transcription job {job_name_vocabulary_list}.")
    # start_job(
    #     job_name_vocabulary_list, media_uri, 'mp3', 'en-US', transcribe_client,
    #     vocabulary_name)
    # transcribe_waiter.wait(job_name_vocabulary_list)
    # job_vocabulary_list = get_job(job_name_vocabulary_list, transcribe_client)
    # transcript_vocabulary_list = requests.get(
    #     job_vocabulary_list['Transcript']['TranscriptFileUri']).json()
    # print(f"Transcript for job {transcript_vocabulary_list['jobName']}:")
    # print(transcript_vocabulary_list['results']['transcripts'][0]['transcript'])

    # print('-'*88)
    # print("Updating the custom vocabulary with table data that provides additional "
    #       "pronunciation hints.")
    # table_vocab_file = 'jabber-vocabulary-table.txt'
    # bucket.upload_file(table_vocab_file, table_vocab_file)
    # update_vocabulary(
    #     vocabulary_name, 'en-US', transcribe_client,
    #     table_uri=f's3://{bucket.name}/{table_vocab_file}')
    # vocabulary_ready_waiter.wait(vocabulary_name)

    # job_name_vocab_table = f'Jabber-vocab-table-{time.time_ns()}'
    # print(f"Starting transcription job {job_name_vocab_table}.")
    # start_job(
    #     job_name_vocab_table, media_uri, 'mp3', 'he-IL', transcribe_client,
    #     vocabulary_name=vocabulary_name)
    # transcribe_waiter.wait(job_name_vocab_table)
    # job_vocab_table = get_job(job_name_vocab_table, transcribe_client)
    # transcript_vocab_table = requests.get(
    #     job_vocab_table['Transcript']['TranscriptFileUri']).json()
    # print(f"Transcript for job {transcript_vocab_table['jobName']}:")
    # print(transcript_vocab_table['results']['transcripts'][0]['transcript'])

    print('-'*88)
    print("Getting data for jobs and vocabularies.")
    jabber_jobs = list_jobs('Jabber', transcribe_client)
    print(f"Found {len(jabber_jobs)} jobs:")
    for job_sum in jabber_jobs:
        job = get_job(job_sum['TranscriptionJobName'], transcribe_client)
        print(f"\t{job['TranscriptionJobName']}, {job['Media']['MediaFileUri']}, "
              f"{job['Settings'].get('VocabularyName')}")

    jabber_vocabs = list_vocabularies('Jabber', transcribe_client)
    print(f"Found {len(jabber_vocabs)} vocabularies:")
    for vocab_sum in jabber_vocabs:
        vocab = get_vocabulary(vocab_sum['VocabularyName'], transcribe_client)
        vocab_content = requests.get(vocab['DownloadUri']).text
        print(f"\t{vocab['VocabularyName']} contents:")
        print(vocab_content)

    print('-'*88)
    # for job_name in [job_name_simple]:
    #     delete_job(job_name, transcribe_client)
    # delete_vocabulary(transcribe_client)
    # bucket.objects.delete()
    # bucket.delete()


if __name__ == '__main__':
    import argparse

    try:
        parser = argparse.ArgumentParser(description=
                                         "This script transcribes all mp3 files in given working dir (wd)")

        # parser.add_argument('-key_id', help='aws access key id', required=True)
        # parser.add_argument('-key_value', help='aws access key id value', required=True)
        parser.add_argument('-language_code', help='specify language_code', required=False)
        parser.add_argument('-wd', help='specify root directory from which to run script', required=True)

        args = vars(parser.parse_args())
        # aws_transcribe_loop_dir(args['wd'], args.get('language_code', None))
        # convert_to_mp3(args['wd'])
        # remove_buckets()
        parse_transcription_results(args['wd'])


    except Exception as e:
        logging.error("error occured: {}".format(e))
