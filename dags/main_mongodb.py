from __future__ import annotations

import inspect
import itertools
import os

from airflow import DAG
try:
    # Airflow >= 2.0
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta, timezone
import praw
from prawcore.exceptions import NotFound, Forbidden, Redirect
from tqdm import tqdm
import json
import logging

from pymongo import MongoClient

import metrics

from pathlib import Path

import utils
from utils import praw_retry
import pipelines
import sampling_methods

DATA_ROOT = Path(__file__).resolve().parent.parent

def get_mongo_client() -> MongoClient:
    from airflow.sdk import Variable

    def get_var(key, default=""):
        val = Variable.get(key, default=None)
        if val is not None:
            return val
        return os.environ.get(key, default)

    host     = get_var("MONGO_HOST",     "localhost")
    port     = int(get_var("MONGO_PORT", "27017"))
    user     = get_var("MONGO_USER",     "")
    password = get_var("MONGO_PASSWORD", "")
    db_name  = get_var("MONGO_DB",       "ContentModerationDB")
    auth_src = get_var("MONGO_AUTH_SRC", "admin")

    if user and password:
        uri = f"mongodb://{user}:{password}@{host}:{port}/{db_name}?authSource={auth_src}"
    else:
        uri = f"mongodb://{host}:{port}/{db_name}"

    return MongoClient(uri)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

def sample_task(**kwargs):
    """
    The sample task performs the initial database population based on 'sample_strategy'.

    sample_strategy values:
        'skip' - Skips sampling; for resuming runs  when the database is already populated.
        'random_subreddits' - Gets random Subreddits from ListOfSubreddits.
        'list' - Populates the database from a static list of Subreddit or User names.
    """
    if 'sample_strategy' not in kwargs['config'].keys():
        logging.warning(
            "No sample_strategy configured, skipping sample task. To silence this, set sample_strategy to \'skip\'.")
        sampling_methods.skip()
    elif kwargs['config']['sample_strategy'] == 'skip':
        sampling_methods.skip()
    elif kwargs['config']['sample_strategy'] == 'random_subreddits':
        sampling_methods.sample_random_subreddits(kwargs)
    elif kwargs['config']['sample_strategy'] == 'list':
        sampling_methods.populate_from_list(kwargs)
    else:
        raise NotImplementedError(f"Unknown sample_strategy: {kwargs['config']['sample_strategy']}")

def stream_task(**kwargs):
    client = get_mongo_client()
    db = client["ContentModerationDB"]

    start_time = kwargs['ti'].xcom_pull(key='start_date')

    if start_time is None:
        start_time = datetime.now(timezone.utc)

    duration = timedelta(days=kwargs['config']['stream_duration_days'])

    reddit = utils.Reddit(client_info=kwargs['config']['client_info'])

    if kwargs['config']['stream_type'] == "subreddit":
        streams = {
            sr: {"comments": reddit.subreddit(sr).stream.comments(pause_after=-1, skip_existing=True),
                 "submissions": reddit.subreddit(sr).stream.submissions(pause_after=-1, skip_existing=True)} for
            sr in db["subreddits_static"].distinct("subreddit_name")}
    else:
        streams = {
            poster: {"comments": reddit.redditor(poster).stream.comments(pause_after=-1, skip_existing=True),
                     "submissions": reddit.redditor(poster).stream.submissions(pause_after=-1, skip_existing=True)} for
            poster in db["users_static"].distinct("user_name")}

    print(f"Initialized {len(streams)} streams.")

    def get_comments(stream):
        comment_list = []
        try:
            for comment in stream['comments']:
                if comment is not None:
                    comment_list.append(comment)
                else:
                    return comment_list
            return comment_list
        except:  # User is suspended
            return comment_list

    def get_submissions(stream):
        submission_list = []
        try:
            for submission in stream['submissions']:
                if submission is not None:
                    submission_list.append(submission)
                else:
                    return submission_list
            return submission_list
        except:  # User is suspended
            return submission_list


    run_id = kwargs['run_id']

    state_doc = db["stream_state"].find_one({"run_id": run_id})
    if state_doc is None:
        state_doc = {
            "run_id": run_id,
            "posts_today": {},
            "streaming_has_finished": False,
            "last_loop_duration_seconds": None,
        }
        db["stream_state"].insert_one(state_doc)

    posts_today = state_doc["posts_today"]

    start_date = kwargs['ti'].xcom_pull(key='start_date')

    if start_date is None:
        start_date = datetime.now(timezone.utc).date()

    def _save_state(posts_today, finished, loop_duration_seconds=None):
        db["stream_state"].update_one(
            {"run_id": run_id},
            {"$set": {
                "posts_today": posts_today,
                "streaming_has_finished": finished,
                "last_loop_duration_seconds": loop_duration_seconds,
                "updated_at": datetime.now(timezone.utc),
            }}
        )

    while datetime.now(timezone.utc) - start_time < duration:

        post_count = 0
        today = datetime.now(timezone.utc).date()
        today_str = today.isoformat()

        loop_start = datetime.now(timezone.utc)
        for k in streams.keys():
            if (k not in posts_today.keys()) or (posts_today[k]["date"] != today_str):
                posts_today[k] = {"date": today_str, "count": 0}
                _save_state(posts_today, finished=False)
            if posts_today[k]["count"] >= kwargs['config']['max_posts_per_stream_per_day']:
                continue
            try:
                stream = streams[k]

                comments = [comment for comment in get_comments(stream) if
                            comment is not None]
                submissions = [submission for submission in get_submissions(stream) if
                               submission is not None]

                total_posts = len(comments) + len(submissions)

                allowed_posts = kwargs['config']['max_posts_per_stream_per_day'] - posts_today[k]["count"]

                if total_posts > allowed_posts:
                    comments = comments[:allowed_posts]
                    submissions = submissions[:max(0, allowed_posts - len(comments))]
                    total_posts = len(comments) + len(submissions)

                posts_today[k]["count"] += total_posts
                _save_state(posts_today, finished=False)

                post_count += total_posts
                for post in itertools.chain(comments, submissions):

                    created_utc = metrics.post_created_utc(post)

                    if datetime.now(timezone.utc) - datetime.utcfromtimestamp(created_utc).replace(
                            tzinfo=timezone.utc) <= timedelta(
                        hours=kwargs['config'][
                            'monitor_interval_hours']):  # post have been made within the monitoring interval

                        post_static_entry = {metric_name: getattr(metrics, metric_name)(post) for
                                             metric_name in
                                             kwargs['config']['post_metrics_static']}

                        static_result = db["posts_static"].insert_one(
                            post_static_entry
                        )

                        post_static_id = static_result.inserted_id

                        post_dynamic_entry = {
                            metric_name: (
                                getattr(metrics, metric_name)(post, return_default=True)
                                if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                                else getattr(metrics, metric_name)(post)
                            )
                            for metric_name in kwargs['config']['post_metrics_dynamic']
                        }

                        scrape_time = metrics.post_created_utc(post)
                        if isinstance(scrape_time, (int, float)):
                            scrape_time = datetime.fromtimestamp(scrape_time, timezone.utc)
                        if isinstance(scrape_time, str):  # scrape_time returned an error
                            scrape_time = datetime.now(timezone.utc)

                        post_dynamic_entry["scrape_time"] = scrape_time
                        post_dynamic_entry["post_ref"] = post_static_id

                        db["posts_dynamic"].insert_one(
                            post_dynamic_entry
                        )

                        # If subreddit not already in database, add it
                        if db['subreddits_static'].find_one({"subreddit_name": post.subreddit.display_name}) is None:
                            subreddit_static_entry = {metric_name: getattr(metrics, metric_name)(post.subreddit) for
                                                      metric_name in
                                                      kwargs['config']['subreddit_metrics_static']}

                            static_result = db["subreddits_static"].insert_one(
                                subreddit_static_entry
                            )

                            subreddit_dynamic_entry = {metric_name: getattr(metrics, metric_name)(post.subreddit) for
                                                       metric_name in
                                                       kwargs['config']['subreddit_metrics_dynamic']}

                            subreddit_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                            subreddit_dynamic_entry["subreddit_ref"] = static_result.inserted_id

                            db["subreddits_dynamic"].insert_one(
                                subreddit_dynamic_entry
                            )

                            if "subreddit_mods_info" in subreddit_dynamic_entry:
                                for mod in subreddit_dynamic_entry['subreddit_mods_info']:
                                    if db['moderators_static'].find_one(
                                            {"user_name": mod['user_name']}) is None:  # not already in DB

                                        mod = utils.get_redditor(reddit, mod['user_name'])

                                        if isinstance(mod,
                                                      praw.models.Redditor):  # Ensures a valid redditor is returned

                                            mod_static_entry = {metric_name: getattr(metrics, metric_name)(mod) for
                                                                metric_name in
                                                                kwargs['config']['moderator_metrics_static']}

                                            static_result = db["moderators_static"].insert_one(
                                                mod_static_entry
                                            )

                                            mod_static_id = static_result.inserted_id

                                            mod_dynamic_entry = {
                                                metric_name: (
                                                    getattr(metrics, metric_name)(mod, return_default=True)
                                                    if "return_default" in inspect.signature(
                                                        getattr(metrics, metric_name)).parameters
                                                    else getattr(metrics, metric_name)(mod)
                                                )
                                                for metric_name in kwargs['config']['moderator_metrics_dynamic']
                                            }

                                            mod_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                                            mod_dynamic_entry["mod_ref"] = mod_static_id

                                            db["moderators_dynamic"].insert_one(
                                                mod_dynamic_entry
                                            )

            except (NotFound, Forbidden, Redirect):  # Stream source no longer good.
                streams.remove(k)
        
        loop_end = datetime.now(timezone.utc)
        loop_duration_seconds = (loop_end - loop_start).total_seconds()
        _save_state(posts_today, finished=False, loop_duration_seconds=loop_duration_seconds)

    _save_state(posts_today, finished=True, loop_duration_seconds=loop_duration_seconds)


@praw_retry
def get_comment(reddit, id):
    return reddit.comment(id=id)


@praw_retry
def get_submission(reddit, id):
    return reddit.submission(id=id)


def monitor_task(**kwargs):
    monitoring_finished = False

    client = get_mongo_client()

    db = client["ContentModerationDB"]

    run_id = kwargs['run_id']

    while True:
        state_doc = db["stream_state"].find_one({"run_id": run_id})
        streaming_finished = state_doc.get("streaming_has_finished", False) if state_doc else False

        if streaming_finished and monitoring_finished:
            break

        reddit = utils.Reddit(client_info=kwargs['config']['client_info'])

        # Post Monitoring
        monitorable_posts = list(db['posts_dynamic'].aggregate(
            pipelines.monitorable_post_pipeline(kwargs['config']['n_monitors'],
                                                kwargs['config']['monitor_interval_hours'])))

        for monitorable_post in monitorable_posts:
            if monitorable_post['post_type'] == 'comment':
                post = get_comment(reddit, monitorable_post['post_id'])
            elif monitorable_post['post_type'] == 'submission':
                post = get_submission(reddit, monitorable_post['post_id'])

            post_dynamic_entry = {
                metric_name: (
                    getattr(metrics, metric_name)(post, return_default=True)
                    if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                    else getattr(metrics, metric_name)(post)
                )
                for metric_name in kwargs['config']['post_metrics_dynamic']
            }
            post_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
            post_dynamic_entry["post_ref"] = monitorable_post['post_ref']

            db["posts_dynamic"].insert_one(
                post_dynamic_entry
            )

        # Subreddit Monitoring
        monitorable_subreddits = list(db['posts_static'].aggregate(
            pipelines.monitorable_subreddit_pipeline(kwargs['config']['n_monitors'],
                                                     kwargs['config']['monitor_interval_hours'])))

        if len(monitorable_subreddits) > 0:
            print(f"Found {len(monitorable_subreddits)} monitorable subreddits.")

        for monitorable_subreddit in monitorable_subreddits:
            sr = utils.get_subreddit(reddit, monitorable_subreddit['subreddit_name'])

            subreddit_dynamic_entry = {
                metric_name: (
                    getattr(metrics, metric_name)(sr, return_default=True)
                    if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                    else getattr(metrics, metric_name)(sr)
                )
                for metric_name in kwargs['config']['subreddit_metrics_dynamic']
            }
            subreddit_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
            subreddit_dynamic_entry["subreddit_ref"] = monitorable_subreddit['subreddit_ref']

            db["subreddits_dynamic"].insert_one(
                subreddit_dynamic_entry
            )

        # Moderator Monitoring
        monitorable_mods = list(db['moderators_dynamic'].aggregate(
            pipelines.monitorable_mod_pipeline_simple(kwargs['config']['n_monitors'],
                                                kwargs['config']['monitor_interval_hours'])))

        if len(monitorable_mods) > 0:
            print(f"Found {len(monitorable_mods)} monitorable moderators.")

        for monitorable_mod in monitorable_mods:
            mod = utils.get_redditor(reddit, monitorable_mod['_id'])

            moderator_dynamic_entry = {
                metric_name: (
                    getattr(metrics, metric_name, return_default=True)(mod)
                    if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                    else getattr(metrics, metric_name)(mod)
                )
                for metric_name in kwargs['config']['moderator_metrics_dynamic']
            }
            moderator_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
            moderator_dynamic_entry["mod_ref"] = monitorable_mod['mod_ref']

            db["moderators_dynamic"].insert_one(
                moderator_dynamic_entry
            )

        monitoring_finished = (len(monitorable_posts) == 0)


with DAG(
        'content_moderation_data_collection',
        default_args=default_args,
        description='Sample usernames, stream posts, and monitor variables.',
        catchup=False,
) as dag:
    with open(DATA_ROOT / 'config.json', 'r') as f:
        config = json.load(f)

    print(DATA_ROOT / 'config.json')
    print(config)
    sample_task_operator = PythonOperator(
        task_id='sample',
        python_callable=sample_task,
        op_kwargs={
            'config': config
        },
    )

    stream_task_operator = PythonOperator(
        task_id='stream',
        python_callable=stream_task,
        op_kwargs={
            'config': config
        },
    )

    monitor_task_operator = PythonOperator(
        task_id='monitor',
        python_callable=monitor_task,
        op_kwargs={
            'config': config
        },
    )

    sample_task_operator >> [stream_task_operator, monitor_task_operator]
