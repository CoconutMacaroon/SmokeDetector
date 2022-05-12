# coding=utf-8
from spamhandling import handle_spam, check_if_spam
from datahandling import (add_or_update_api_data, clear_api_data, schedule_store_bodyfetcher_queue,
                          schedule_store_bodyfetcher_max_ids, add_queue_timing_data)
from chatcommunicate import tell_rooms_with
from globalvars import GlobalVars
from operator import itemgetter
from datetime import datetime
import json
import time
import threading
import requests
import copy
from classes import Post, PostParseError
from helpers import (log, log_current_thread, append_to_current_thread_name,
                     convert_new_scan_to_spam_result_if_new_reasons, add_to_global_bodyfetcher_queue_in_new_thread)
import recently_scanned_posts as rsp
from itertools import chain
from tasks import Tasks


# noinspection PyClassHasNoInit,PyBroadException
class BodyFetcher:
    queue = {}
    previous_max_ids = {}
    posts_in_process = {}

    # special_cases are the minimum number of posts, for each of the specified sites, which
    # need to be in the queue prior to feching posts.
    # The number of questions we fetch each day per site is the total of
    #   new questions + new answers + edits.
    # Stack Overflow is handled specially. It's know that some SO edits/new posts don't
    #   appear on the WebSocket.
    # Queue depths were last comprehensively adjusted on 2015-12-30.
    special_cases = {
        # 2020-11-02:
        # Request numbers pre 2020-11-02 are very low due to a now fixed bug.
        #
        #                                                                pre                   sum of requests
        #                                               questions    2020-11-02   2020-02-19    2020-10-28 to
        #                                                per day       setting     requests      2020-11-02
        # "stackoverflow.com": 3,                   # _  6,816            3          360            4,365
        # "math.stackexchange.com": 2,              # _    596            1          473            6,346
        # "ru.stackoverflow.com": 2,                # _    230           10           13              145
        # "askubuntu.com": ,                        # _    140            1           88            1,199
        # "es.stackoverflow.com": 2,                # _    138            5           25              225
        # "superuser.com": ,                        # _    122            1           87            1,038
        # "physics.stackexchange.com": ,            # _     90            1           76            1,161
        # "stats.stackexchange.com": 2,             # _     82            5           16              151
        # "pt.stackoverflow.com": 2,                # _     73           10            7               75
        # "unix.stackexchange.com": ,               # _     72            1           76              772
        # "electronics.stackexchange.com": ,        # _     69            1           46              723
        # "serverfault.com": ,                      # _     62            1           43              582
        # "tex.stackexchange.com": 2,               # _     60            5            8               98
        # "blender.stackexchange.com": 2,           # _     59            5            8               85
        # "salesforce.stackexchange.com": ,         # _     49            1           47              472
        # "gis.stackexchange.com": 2,               # _     46            3           15              166
        # "mathoverflow.net" (time_sensitive)       # _     37            -           33              511
        # "english.stackexchange.com": ,            # _     36            1           34              382
        # "magento.stackexchange.com": 2,           # _     34            3            5               93
        # "ell.stackexchange.com": ,                # _     33            1           24              365
        # "wordpress.stackexchange.com": ,          # _     29            1           30              283
        # "apple.stackexchange.com": ,              # _     29            1           46              294
        # "diy.stackexchange.com": ,                # _     26            1           24              306
        # "mathematica.stackexchange.com": ,        # _     25            1           21              384
        # "dba.stackexchange.com": ,                # _     23            1           31              343
        # "datascience.stackexchange.com": ,        # _     21            1           17              220
        # "chemistry.stackexchange.com": ,          # _     20            1           20              140
        # "security.stackexchange.com": ,           # _     18            1           15              238
        # "codereview.stackexchange.com": ,         # _     18            5            2               39
        #  The only reason this is the cut-off is that it was the last in the existing list
        #    as of 2020-11-01.
    }

    time_sensitive = ["security.stackexchange.com", "movies.stackexchange.com",
                      "mathoverflow.net", "gaming.stackexchange.com", "webmasters.stackexchange.com",
                      "arduino.stackexchange.com", "workplace.stackexchange.com"]

    threshold = 1

    last_activity_date = 0
    last_activity_date_lock = threading.Lock()
    ACTIVITY_DATE_EXTRA_EARLIER_MS_TO_FETCH = 6 * 60 * 1000  # 6 minutes in milliseconds; is beyond edit grace period

    api_data_lock = threading.Lock()
    queue_lock = threading.Lock()
    max_ids_modify_lock = threading.Lock()
    check_queue_lock = threading.Lock()
    posts_in_process_lock = threading.Lock()

    def add_to_queue(self, hostname, question_id, should_check_site=False):
        # For the Sandbox questions on MSE, we choose to ignore the entire question and all answers.
        ignored_mse_questions = [
            3122,    # Formatting Sandbox
            51812,   # The API sandbox
            296077,  # Sandbox archive
        ]
        if question_id in ignored_mse_questions and hostname == "meta.stackexchange.com":
            return  # don't check meta sandbox, it's full of weird posts

        with self.queue_lock:
            if hostname not in self.queue:
                self.queue[hostname] = {}

            # Something about how the queue is being filled is storing Post IDs in a list.
            # So, if we get here we need to make sure that the correct types are paseed.
            #
            # If the item in self.queue[hostname] is a dict, do nothing.
            # If the item in self.queue[hostname] is not a dict but is a list or a tuple, then convert to dict and
            # then replace the list or tuple with the dict.
            # If the item in self.queue[hostname] is neither a dict or a list, then explode.
            if type(self.queue[hostname]) is dict:
                pass
            elif type(self.queue[hostname]) in [list, tuple]:
                post_list_dict = {}
                for post_list_id in self.queue[hostname]:
                    post_list_dict[str(post_list_id)] = None
                self.queue[hostname] = post_list_dict
            else:
                raise TypeError("A non-iterable is in the queue item for a given site, this will cause errors!")

            # This line only works if we are using a dict in the self.queue[hostname] object, which we should be with
            # the previous conversion code.
            self.queue[hostname][str(question_id)] = datetime.utcnow()
            flovis_dict = None
            if GlobalVars.flovis is not None:
                flovis_dict = {sk: list(sq.keys()) for sk, sq in self.queue.items()}

        if flovis_dict is not None:
            GlobalVars.flovis.stage('bodyfetcher/enqueued', hostname, question_id, flovis_dict)

        if should_check_site:
            # The call to add_to_queue indicated that the site should be immediately processed.
            with self.queue_lock:
                new_posts = self.queue.pop(hostname, None)
            if new_posts:
                schedule_store_bodyfetcher_queue()
                self.make_api_call_for_site(hostname, new_posts)

        site_and_posts = self.get_fist_queue_item_to_process()
        if site_and_posts:
            schedule_store_bodyfetcher_queue()
            self.make_api_call_for_site(*site_and_posts)

    def get_fist_queue_item_to_process(self):
        # We use a copy of the queue keys (sites) and lengths in order to allow
        # the queue to be changed in other threads.
        # Overall this results in a FIFO for sites which have reached their threshold, because
        # dicts are guaranteed to be iterated in insertion order in Python >= 3.6.
        # We use self.check_queue_lock here to fully dispatch one queued site at a time and allow
        # consolidation of multiple WebSocket events for the same real-world event.
        with self.check_queue_lock:
            time.sleep(.25)  # Some time for multiple potential  WebSocket events to queue the same post
            special_sites = []
            site_to_handle = None
            is_time_sensitive_time = datetime.utcnow().hour in range(4, 12)
            with self.queue_lock:
                sites_in_queue = {site: len(values) for site, values in self.queue.items()}
            # Get sites listed in special cases and as time_sensitive
            for site, length in sites_in_queue.items():
                if site in self.special_cases:
                    special_sites.append(site)
                    if length >= self.special_cases[site]:
                        site_to_handle = site
                        break
                if is_time_sensitive_time and site in self.time_sensitive:
                    special_sites.append(site)
                    if length >= 1:
                        site_to_handle = site
                        break
            else:
                # We didn't find a special site which met the applicable threshold.
                # Remove the sites which we've already considered from our copy of the queue's keys.
                for site in special_sites:
                    sites_in_queue.pop(site, None)

                # If we don't have any special sites with their queue filled, take the first
                # one without a special case
                for site, length in sites_in_queue.items():
                    if length >= self.threshold:
                        site_to_handle = site
                        break

            if site_to_handle is not None:
                with self.queue_lock:
                    new_posts = self.queue.pop(site_to_handle, None)
                if new_posts:
                    # We've identified a site and have a list of new posts to fetch.
                    return (site, new_posts)
            # There's no site in the queue which has met the applicable threshold.
            return None

    def print_queue(self):
        with self.queue_lock:
            if self.queue:
                return '\n'.join(["{0}: {1}".format(key, str(len(values))) for (key, values) in self.queue.items()])
            else:
                return 'The BodyFetcher queue is empty.'

    def claim_post_in_process_or_request_rescan(self, ident, site, post_id):
        with self.posts_in_process_lock:
            site_dict = self.posts_in_process.get(site, None)
            if site_dict is None:
                site_dict = {}
                self.posts_in_process[site] = site_dict
            post_dict = site_dict.get(post_id, None)
            if post_dict is None:
                post_dict = {
                    'owner': ident,
                    'first_timestamp': time.time(),
                }
                site_dict[post_id] = post_dict
                return True
            if post_dict.get('owner', None) == ident:
                post_dict['recent_timestamp'] = time.time(),
                return True
            post_dict['rescan_requested'] = True
            post_dict['rescan_requested_by'] = ident
            return False

    def release_post_in_process_and_recan_if_requested(self, ident, site, post_id, question_id):
        with self.posts_in_process_lock:
            site_dict = self.posts_in_process[site]
            post_dict = site_dict[post_id]
            if post_dict['owner'] == ident:
                if post_dict.get('rescan_requested', None) is True:
                    add_to_global_bodyfetcher_queue_in_new_thread(site, question_id, False,
                                                                  source="BodyFetcher re-request")
                site_dict.pop(post_id, None)
                return True
            # There's really nothing for us to do here. We could raise an error, but it's
            # unclear that would help this thread.
            return False

    def make_api_call_for_site(self, site, new_posts):
        current_thread_ident = threading.current_thread().ident
        append_to_current_thread_name(' --> processing site: {}:: posts: {}'.format(site,
                                                                                    [key for key in new_posts.keys()]))

        new_post_ids = [int(k) for k in new_posts.keys()]
        Tasks.do(GlobalVars.edit_watcher.subscribe, hostname=site, question_id=new_post_ids)

        if GlobalVars.flovis is not None:
            for post_id in new_post_ids:
                GlobalVars.flovis.stage('bodyfetcher/api_request', site, post_id,
                                        {'site': site, 'posts': list(new_posts.keys())})

        # Add queue timing data
        pop_time = datetime.utcnow()
        post_add_times = [(pop_time - v).total_seconds() for k, v in new_posts.items()]
        Tasks.do(add_queue_timing_data, site, post_add_times)

        store_max_ids = False
        with self.max_ids_modify_lock:
            if site in self.previous_max_ids and max(new_post_ids) > self.previous_max_ids[site]:
                previous_max_id = self.previous_max_ids[site]
                intermediate_posts = range(previous_max_id + 1, max(new_post_ids))

                # We don't want to go over the 100-post API cutoff, so take the last
                # (100-len(new_post_ids)) from intermediate_posts

                intermediate_posts = intermediate_posts[-(100 - len(new_post_ids)):]

                # new_post_ids could contain edited posts, so merge it back in
                combined = chain(intermediate_posts, new_post_ids)

                # Could be duplicates, so uniquify
                posts = list(set(combined))
            else:
                posts = new_post_ids

            new_post_ids_max = max(new_post_ids)
            if new_post_ids_max > self.previous_max_ids.get(site, 0):
                self.previous_max_ids[site] = new_post_ids_max
                store_max_ids = True

        if store_max_ids:
            schedule_store_bodyfetcher_max_ids()

        log('debug', "New IDs / Hybrid Intermediate IDs for {}:".format(site))
        if len(new_post_ids) > 30:
            log('debug', "{} +{} more".format(sorted(new_post_ids)[:30], len(new_post_ids) - 30))
        else:
            log('debug', sorted(new_post_ids))
        if len(new_post_ids) == len(posts):
            log('debug', "[ *Identical* ]")
        elif len(posts) > 30:
            log('debug', "{} +{} more".format(sorted(posts)[:30], len(posts) - 30))
        else:
            log('debug', sorted(posts))

        question_modifier = ""
        pagesize_modifier = {}

        if site == "stackoverflow.com":
            # Not all SO questions are shown in the realtime feed. We now
            # fetch all recently modified SO questions to work around that.
            with self.last_activity_date_lock:
                if self.last_activity_date != 0:
                    pagesize = "100"
                else:
                    pagesize = "50"

                pagesize_modifier = {
                    'pagesize': pagesize,
                    'min': str(self.last_activity_date - self.ACTIVITY_DATE_EXTRA_EARLIER_MS_TO_FETCH)
                }
        else:
            question_modifier = "/{0}".format(";".join([str(post) for post in posts]))

        url = "https://api.stackexchange.com/2.2/questions{}".format(question_modifier)
        params = {
            'filter': '!1rs)sUKylwB)8isvCRk.xNu71LnaxjnPS12*pX*CEOKbPFwVFdHNxiMa7GIVgzDAwMa',
            'key': 'IAkbitmze4B8KpacUfLqkw((',
            'site': site
        }
        params.update(pagesize_modifier)

        # wait to make sure API has/updates post data
        time.sleep(3)

        with GlobalVars.api_request_lock:
            # Respect backoff, if we were given one
            if GlobalVars.api_backoff_time > time.time():
                time.sleep(GlobalVars.api_backoff_time - time.time() + 2)
            try:
                time_request_made = datetime.utcnow().strftime('%H:%M:%S')
                response = requests.get(url, params=params, timeout=20).json()
                response_timestamp = time.time()
            except (requests.exceptions.Timeout, requests.ConnectionError, Exception):
                # Any failure in the request being made (timeout or otherwise) should be added back to
                # the queue.
                with self.queue_lock:
                    if site in self.queue:
                        self.queue[site].update(new_posts)
                    else:
                        self.queue[site] = new_posts
                return

            with self.api_data_lock:
                add_or_update_api_data(site)

            message_hq = ""
            with GlobalVars.apiquota_rw_lock:
                if "quota_remaining" in response:
                    quota_remaining = response["quota_remaining"]
                    if quota_remaining - GlobalVars.apiquota >= 5000 and GlobalVars.apiquota >= 0 \
                            and quota_remaining > 39980:
                        tell_rooms_with("debug", "API quota rolled over with {0} requests remaining. "
                                                 "Current quota: {1}.".format(GlobalVars.apiquota,
                                                                              quota_remaining))

                        sorted_calls_per_site = sorted(GlobalVars.api_calls_per_site.items(), key=itemgetter(1),
                                                       reverse=True)
                        api_quota_used_per_site = ""
                        for site_name, quota_used in sorted_calls_per_site:
                            sanatized_site_name = site_name.replace('.com', '').replace('.stackexchange', '')
                            api_quota_used_per_site += sanatized_site_name + ": {0}\n".format(str(quota_used))
                        api_quota_used_per_site = api_quota_used_per_site.strip()

                        tell_rooms_with("debug", api_quota_used_per_site)
                        clear_api_data()
                    if quota_remaining == 0:
                        tell_rooms_with("debug", "API reports no quota left!  May be a glitch.")
                        tell_rooms_with("debug", str(response))  # No code format for now?
                    if GlobalVars.apiquota == -1:
                        tell_rooms_with("debug", "Restart: API quota is {quota}."
                                                 .format(quota=quota_remaining))
                    GlobalVars.apiquota = quota_remaining
                else:
                    message_hq = "The quota_remaining property was not in the API response."

            if "error_message" in response:
                message_hq += " Error: {} at {} UTC.".format(response["error_message"], time_request_made)
                if "error_id" in response and response["error_id"] == 502:
                    if GlobalVars.api_backoff_time < time.time() + 12:  # Add a backoff of 10 + 2 seconds as a default
                        GlobalVars.api_backoff_time = time.time() + 12
                message_hq += " Backing off on requests for the next 12 seconds."
                message_hq += " Previous URL: `{}`".format(url)

            if "backoff" in response:
                if GlobalVars.api_backoff_time < time.time() + response["backoff"]:
                    GlobalVars.api_backoff_time = time.time() + response["backoff"]

        if len(message_hq) > 0 and "site is required" not in message_hq:
            message_hq = message_hq.strip()
            if len(message_hq) > 500:
                message_hq = "\n" + message_hq
            tell_rooms_with("debug", message_hq)

        if "items" not in response:
            return

        if site == "stackoverflow.com":
            items = response["items"]
            if len(items) > 0 and "last_activity_date" in items[0]:
                with self.last_activity_date_lock:
                    self.last_activity_date = items[0]["last_activity_date"]

        num_scanned = 0
        start_time = time.time()

        for post in response["items"]:
            if GlobalVars.flovis is not None:
                pnb = copy.deepcopy(post)
                if 'body' in pnb:
                    pnb['body'] = 'Present, but truncated'
                if 'answers' in pnb:
                    del pnb['answers']

            if "title" not in post or "body" not in post:
                if GlobalVars.flovis is not None and 'question_id' in post:
                    GlobalVars.flovis.stage('bodyfetcher/api_response/no_content', site, post['question_id'], pnb)
                continue

            post['site'] = site
            post['response_timestamp'] = response_timestamp
            try:
                post['edited'] = (post['creation_date'] != post['last_edit_date'])
            except KeyError:
                post['edited'] = False  # last_edit_date not present = not edited

            question_id = post.get('question_id', None)
            if question_id is not None:
                Tasks.do(GlobalVars.edit_watcher.subscribe, hostname=site, question_id=question_id)
            try:
                if self.claim_post_in_process_or_request_rescan(current_thread_ident, site, question_id):
                    compare_info = rsp.atomic_compare_update_and_get_spam_data(post)
                    question_doesnt_need_scan = compare_info['is_older_or_unchanged']
                else:
                    question_doesnt_need_scan = True

                if question_doesnt_need_scan and "answers" not in post:
                    continue
                do_flovis = GlobalVars.flovis is not None and question_id is not None
                try:
                    post_ = Post(api_response=post)
                except PostParseError as err:
                    log('error', 'Error {0} when parsing post: {1!r}'.format(err, post_))
                    if do_flovis:
                        GlobalVars.flovis.stage('bodyfetcher/api_response/error', site, question_id, pnb)
                    continue

                if not question_doesnt_need_scan:
                    num_scanned += 1
                    is_spam, reason, why = convert_new_scan_to_spam_result_if_new_reasons(check_if_spam(post_),
                                                                                          compare_info)
                    rsp.add_post(post, is_spam=is_spam, reasons=reason, why=why)

                    if is_spam:
                        try:
                            if do_flovis:
                                GlobalVars.flovis.stage('bodyfetcher/api_response/spam', site, question_id,
                                                        {'post': pnb, 'check_if_spam': [is_spam, reason, why]})
                            handle_spam(post=post_,
                                        reasons=reason,
                                        why=why)
                        except Exception as e:
                            log('error', "Exception in handle_spam:", e)
                    elif do_flovis:
                        GlobalVars.flovis.stage('bodyfetcher/api_response/not_spam', site, question_id,
                                                {'post': pnb, 'check_if_spam': [is_spam, reason, why]})
            except Exception:
                raise
            finally:
                self.release_post_in_process_and_recan_if_requested(current_thread_ident, site, question_id,
                                                                    question_id)

            try:
                if "answers" not in post:
                    pass
                else:
                    for answer in post["answers"]:
                        if GlobalVars.flovis is not None:
                            anb = copy.deepcopy(answer)
                            if 'body' in anb:
                                anb['body'] = 'Present, but truncated'

                        answer['response_timestamp'] = response_timestamp
                        answer["IsAnswer"] = True  # Necesssary for Post object
                        answer["title"] = ""  # Necessary for proper Post object creation
                        answer["site"] = site  # Necessary for proper Post object creation
                        try:
                            answer['edited'] = (answer['creation_date'] != answer['last_edit_date'])
                        except KeyError:
                            answer['edited'] = False  # last_edit_date not present = not edited
                        answer_id = answer.get('answer_id', None)
                        try:
                            if self.claim_post_in_process_or_request_rescan(current_thread_ident, site, answer_id):
                                compare_info = rsp.atomic_compare_update_and_get_spam_data(answer)
                                answer_doesnt_need_scan = compare_info['is_older_or_unchanged']
                            else:
                                continue
                            if answer_doesnt_need_scan:
                                continue
                            num_scanned += 1
                            answer_ = Post(api_response=answer, parent=post_)

                            raw_results = check_if_spam(answer_)
                            is_spam, reason, why = convert_new_scan_to_spam_result_if_new_reasons(raw_results,
                                                                                                  compare_info)
                            rsp.add_post(answer, is_spam=is_spam, reasons=reason, why=why)
                            if is_spam:
                                do_flovis = GlobalVars.flovis is not None and answer_id is not None
                                try:
                                    if do_flovis:
                                        GlobalVars.flovis.stage('bodyfetcher/api_response/spam', site, answer_id,
                                                                {'post': anb, 'check_if_spam': [is_spam, reason, why]})
                                    handle_spam(answer_,
                                                reasons=reason,
                                                why=why)
                                except Exception as e:
                                    log('error', "Exception in handle_spam:", e)
                            elif do_flovis:
                                GlobalVars.flovis.stage('bodyfetcher/api_response/not_spam', site, answer_id,
                                                        {'post': anb, 'check_if_spam': [is_spam, reason, why]})
                        except Exception:
                            raise
                        finally:
                            self.release_post_in_process_and_recan_if_requested(current_thread_ident, site, answer_id,
                                                                                question_id)

            except Exception as e:
                log('error', "Exception handling answers:", e)

        end_time = time.time()
        scan_time = end_time - start_time
        GlobalVars.PostScanStat.add_stat(num_scanned, scan_time)
        return
