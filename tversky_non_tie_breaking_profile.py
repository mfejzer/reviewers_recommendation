import collections 
import json
import psutil
import sys
from operator import itemgetter
from tqdm import tqdm

def tversky_params(reviewers, commit_count_words):
    d1 = sum((reviewers - commit_count_words).values()) 
    d2 = sum((commit_count_words - reviewers).values()) 
    i = sum((commit_count_words & reviewers).values())

    return i, d1, d2

def callculate_tversky_for_reviewers(reviewers, commit_count_words):
    top = collections.defaultdict(list)

    for r in reviewers:
        i, d1, d2 =  tversky_params( reviewers[r], commit_count_words)
        a = 0.0
        m_max = i / ( i + a * d1 + (1-a) * d2)
        top[m_max].append(r)

    sorted_top = sorted(top.keys(), reverse=True)

    return top, sorted_top

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
    commit_count_words = collections.Counter()

    for f in files:
        commit_count_words.update(f.split("/"))
    
    return commit_count_words


def parse_file(f_in):
    with open(f_in) as f: data = [json.loads(line) for line in f]

    data = sorted(data, key=itemgetter('changeId')) 

    prediction = collections.Counter()
    reviewers = collections.defaultdict(collections.Counter)
    suggested_reviewers_count = collections.defaultdict(list)

    mrr_sum = 0.0
    mrr_count = 0.0

    reviews_size = 0.0

    for index,d in enumerate(tqdm(data)):

        commit_count_words = callculate_commit_files_to_words(d["files"])
        top, sorted_top = callculate_tversky_for_reviewers(reviewers, commit_count_words)
        top_dict = get_top(sorted_top, top)

        for k in top_dict:
            suggested_reviewers_count[k].append(len(top_dict[k]))
            for hist in d["approve_history"]:
                if  hist['userId'] in top_dict[k]: 
                    prediction[k] += 1
                    if not reviewers[hist['userId']]:
                        empty_profile += 1
                    break

        for hist in d["approve_history"]:
            reviewers[hist['userId']] += commit_count_words
            reviewer = hist['userId']
            in_mrr = False
            for k in top_dict:
                if reviewer in top_dict[k]: 
                    if not(in_mrr):
                        mrr_sum += 1.0 / k
                        mrr_count += 1
                        in_mrr = True

        reviews_size += 1

    precision = collections.Counter()
    recall = collections.Counter()
    for key, value in prediction.items():
        precision[key] = float(value) / sum(i for i in suggested_reviewers_count[key])
        recall[key] = float(value) / reviews_size

    for p in sorted(prediction): 
        print("Top %d = %f" % (p, float(prediction[p]) / reviews_size))
    print("MRR %f" % (mrr_sum / mrr_count))

    print_precision(precision)
    print_recall(recall)

    current_process = psutil.Process()
    current_memory_info = current_process.memory_info()
    print(current_memory_info) 
    print('####')
    print(suggested_reviewers_count[1])

def print_precision(precision_top):
    print("Precision")
    for n in sorted(precision_top): 
        print("%f" % (float(precision_top[n])))

def print_recall(recall_top):
    print("Recall")
    for n in sorted(recall_top): 
        print("%f" % (float(recall_top[n])))



for f in sys.argv[1:]:
    users = parse_file(f)
