import json
import collections 
import sys
import operator
import math
import psutil

from collections import Counter
from datetime import date, datetime, timedelta
from operator import itemgetter
from time import mktime, strptime
from tqdm import tqdm

class ExtendedCounter(Counter):
    def __mul__(self, ext):
        ext_words = self.copy()
        for k, v in list(ext_words.items()):
            ext_words[k] = v * ext
        return ext_words

    __rmul__ = __mul__

def tversky_params(reviewers, commit_count_words):
    d1 = sum((reviewers - commit_count_words).values()) 
    d2 = sum((commit_count_words - reviewers).values()) 
    i = sum((commit_count_words & reviewers).values())

    return i, d1, d2

def callculate_tversky_for_reviewers(reviewers, commit_count_words, reviewers_last_date, last_if_absent, current, change_ext_function):
    top = collections.defaultdict(list)

    for r in reviewers:
        last = reviewers_last_date.get(r, last_if_absent)
        ext = change_ext_function(last, current)
        ext_words = ExtendedCounter(commit_count_words) * ext

        i, d1, d2 =  tversky_params( reviewers[r], ext_words)
        a = 0.0
        m_max = i / ( i + a * d1 + (1-a) * d2)
        top[m_max].append(r)

    sorted_top = sorted(list(top.keys()), reverse=True)

    return top, sorted_top

def sorted_list_by_date(users, users_last_date):
    selected_users_dates = {u:users_last_date[u] for u in users}

    out = sorted(list(selected_users_dates.items()), key=operator.itemgetter(1), reverse=True)
    return [o[0] for o in out]

def get_top_by_date(sorted_top, top, users_last_date):
    ret_top = collections.defaultdict(list)
    t_ret_top = []

    max_top = len(sorted_top)

    # check if sorted_top not empty and if top1 value >= 0
    if len(sorted_top) == 0 or sorted_top[0] == 0: 
        return ret_top
    
    # Callculate top1..top10, 
    for i in range(0, max_top):
        # use current top if available
        if i < max_top and sorted_top[i] > 0 : 
            l = sorted_list_by_date(top[sorted_top[i]], users_last_date)
            t_ret_top.extend(l)

    for i in range(1, 11):
        # reuse previous top
        ret_top[i].extend(t_ret_top[:i])

    return ret_top

def get_top(sorted_top, top):
    ret_top = collections.defaultdict(list)

    max_top = len(sorted_top)

    # check if sorted_top not empty and if top1 value >= 0
    if len(sorted_top) == 0 or sorted_top[0] == 0: 
        return ret_top
    
    ret_top[1].extend(top[sorted_top[0]])
    
    # Callculate top1..top10, 
    for i in range(1, 10):
        # use current top if available
        if i < max_top and sorted_top[i] > 0 : 
            ret_top[i+1].extend(top[sorted_top[i]])
        # reuse previous top
        ret_top[i+1].extend(ret_top[i])
    
    return ret_top


def callculate_commit_files_to_words(files):
    commit_count_words = ExtendedCounter()

    for f in files:
        commit_count_words.update(f.split("/"))

    return commit_count_words


def parse_file(f_in):
    with open(f_in) as f: data = [json.loads(line) for line in f]

    data = sorted(data, key=itemgetter('changeId')) 

    prediction_id = collections.Counter()
    reviewers_id = collections.defaultdict(collections.Counter)
    reviewers_last_id = collections.defaultdict(None)
    suggested_reviewers_count_id = collections.defaultdict(list)
    mrr_sum_id = 0.0
    mrr_count_id = 0.0


    prediction_date = collections.Counter()
    reviewers_date = collections.defaultdict(collections.Counter)
    reviewers_last_date = collections.defaultdict(None)
    suggested_reviewers_count_date = collections.defaultdict(list)
    mrr_sum_date = 0.0
    mrr_count_date = 0.0

    review_count = 0.0


    for index,d in enumerate(tqdm(data)):
        
        commit_count_words = callculate_commit_files_to_words(d["files"])

        default_last_id = d['changeId'] -1
        current_id = d['changeId']
        top_id, sorted_top_id = callculate_tversky_for_reviewers(reviewers_id, commit_count_words, reviewers_last_id, default_last_id, current_id, change_id_ext) 
        top_dict_id = get_top_by_date(sorted_top_id, top_id, reviewers_last_id)

        default_last_date = convert_date(d['close_date']) - timedelta(days=1)
        current_date = convert_date(d['close_date']) 
        top_date, sorted_top_date = callculate_tversky_for_reviewers(reviewers_date, commit_count_words, reviewers_last_date, default_last_date, current_date, change_date_ext)
        top_dict_date = get_top_by_date(sorted_top_date, top_date, reviewers_last_date)

        for k in top_dict_id:
            suggested_reviewers_count_id[k].append(len(top_dict_id[k]))
            for hist in d["approve_history"]:
                if  hist['userId'] in top_dict_id[k]: 
                    prediction_id[k] += 1
                    break
        for k in top_dict_date:
            suggested_reviewers_count_date[k].append(len(top_dict_date[k]))
            for hist in d["approve_history"]:
                if  hist['userId'] in top_dict_date[k]:
                    prediction_date[k] += 1
                    break

        for hist in d["approve_history"]:
            last_id = reviewers_last_id.get(hist['userId'], d['changeId'] -1)
            current_id = d['changeId']
            ext_id = change_id_ext(last_id, current_id)
            reviewers_id[hist['userId']] += commit_count_words * ext_id
            reviewers_last_id[hist['userId']] = d['changeId']

            last_date = reviewers_last_date.get(hist['userId'], convert_date(hist['grant_date']) - timedelta(days=1))
            current_date = convert_date(hist['grant_date'])
            ext_date = change_date_ext(last_date, current_date)
            reviewers_date[hist['userId']] += commit_count_words * ext_date
            reviewers_last_date[hist['userId']] = convert_date(hist['grant_date'])

            reviewer = hist['userId']
            in_mrr_id = False
            for k in top_dict_id:
                if reviewer in top_dict_id[k]: 
                    if not(in_mrr_id):
                        mrr_sum_id += 1.0 / k
                        mrr_count_id += 1
                        in_mrr_id = True

            in_mrr_date = False
            for k in top_dict_date:
                if reviewer in top_dict_date[k]: 
                    if not(in_mrr_date):
                        mrr_sum_date += 1.0 / k
                        mrr_count_date += 1
                        in_mrr_date = True
            review_count += 1

    precision_id = collections.Counter()
    recall_id = collections.Counter()

    precision_date = collections.Counter()
    recall_date = collections.Counter()

    for key, value in list(prediction_id.items()):
        precision_id[key] = float(value) / sum(i for i in suggested_reviewers_count_id[key])
        recall_id[key] = float(value) / review_count
        prediction_id[key] = float(value) / review_count

    for key, value in list(prediction_date.items()):
        precision_date[key] = float(value) / sum(i for i in suggested_reviewers_count_date[key])
        recall_date[key] = float(value) / review_count
        prediction_date[key] = float(value) / review_count


    print("Id ext")
    for p in sorted(prediction_id): 
        print("Top %d = %f" % (p, float(prediction_id[p])))
    print("MRR %f" % (mrr_sum_id / mrr_count_id))
    print_precision(precision_id)
    print_recall(recall_id)
    print("Date ext")
    for p in sorted(prediction_date): 
        print("Top %d = %f" % (p, float(prediction_date[p])))
    print("MRR %f" % (mrr_sum_date / mrr_count_date))
    print_precision(precision_date)
    print_recall(recall_date)
    
    current_process = psutil.Process()
    current_memory_info = current_process.memory_info()
    print(current_memory_info)
 

def print_precision(precision_top):
    print("Precision")
    for n in sorted(precision_top): 
        print("%f" % (float(precision_top[n])))

def print_recall(recall_top):
    print("Recall")
    for n in sorted(recall_top): 
        print("%f" % (float(recall_top[n])))

def convert_date(date_as_string):
    return datetime.fromtimestamp(mktime(strptime(date_as_string[:-3], "%Y-%m-%d %H:%M:%S.%f")))

def change_id_ext(last, current):
    half_life = 2500 
    # 1/2 after 2000 reviews
    fraction = 0.5
    
    difference = current - last
    
    decay = math.pow(math.pow(fraction, 1.0 / float(half_life)), difference)

    return 1.0 / decay

def change_date_ext(last, current):
    half_life = 183 
    # 1/2 after 183 days
    fraction = 0.5
    
    difference = float((current - last).days)
    
    decay = math.pow(math.pow(fraction, 1.0 / float(half_life)), difference)
    return 1.0 / decay


for f in sys.argv[1:]:
    users = parse_file(f)

