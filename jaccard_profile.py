import collections 
import itertools
import json
import psutil
import sys

from operator import itemgetter
from tqdm import tqdm
from timeit import default_timer

def file_path_to_list(file_path):
    return file_path.split("/")


# 5% speed boost using encode + intern
def fast_file_path_to_list(file_path):
    # with Python 3.x JSON parser returns 'str', not 'bytes'; no need for .encode()
    return [sys.intern(w) for w in file_path_to_list(file_path)]


def parse_file(f_in):
    with open(f_in) as f: reviews = [json.loads(line) for line in f]

    reviews = sorted(reviews, key=itemgetter('changeId'))
    for index, review in enumerate(reviews):
        files = review['files']
        files = list(map(fast_file_path_to_list, files))
        review['files'] = files

    return reviews


def jaccard_params(reviewer_profiles, commit_count_words):
    i  = float(sum((commit_count_words & reviewer_profiles).values()))
    u  = float(sum((commit_count_words | reviewer_profiles).values()))
    return i, u

def jaccard(intersection, union):
    return intersection / union


def jaccard_for_reviewer_profiles(reviewer_profiles, commit_count_words):
    top = collections.defaultdict(list)

    for reviewer_id in reviewer_profiles:
        i, u =  jaccard_params(reviewer_profiles[reviewer_id], commit_count_words)
        m_max = jaccard(i, u)
        top[m_max].append(reviewer_id)

    return top

def commit_files_to_words(file_paths):
    commit_count_words = collections.Counter()

    for file_path_as_list in file_paths:
        commit_count_words.update(file_path_as_list)

    return commit_count_words

def process_reviews(reviews):
    reviewer_profiles = collections.defaultdict(collections.Counter)

    topN = TopN()
    
    total_transformation = 0.0
    total_update = 0.0
    total_similarity = 0.0
    total = 0.0
    before = default_timer()

    for index, review in enumerate(tqdm(reviews)):
        before_transformation = default_timer()
        commit_count_words = commit_files_to_words(review["files"])
        after_transformation = default_timer()
        total_transformation += after_transformation - before_transformation


        before_similarity = default_timer()
        top = jaccard_for_reviewer_profiles(reviewer_profiles, commit_count_words)
        after_similarity = default_timer()
        total_similarity += after_similarity - before_similarity

        topN.update(review, top)

        before_update = default_timer()
        for hist in review["approve_history"]:
            reviewer = hist['userId']
            reviewer_profiles[reviewer] += commit_count_words
        after_update = default_timer()
        total_update += after_update - before_update

    after = default_timer()
    total = after - before

    all_profiles_size = 0.0
    average_profile_size = 0.0
    for reviewer_profile in reviewer_profiles:
        all_profiles_size += sys.getsizeof(reviewer_profiles[reviewer_profile])
    average_profile_size = all_profiles_size / len(reviewer_profiles)

    current_process = psutil.Process()
    current_memory_info = current_process.memory_info()

    return topN.results(), (total, total_transformation, total_update, total_similarity), (all_profiles_size, average_profile_size, current_memory_info)

class TopN:
    def __init__(self):
        self.prediction = collections.Counter()
        self.suggested_reviewers_count = collections.defaultdict(list)
        self.reviewer_last_review_dates = collections.defaultdict(collections.Counter)
        self.reviewer_count = 0.0
        self.mrr_sum = 0.0
        self.mrr_count = 0.0

    def update(self, review, top):
        top_dict = get_top_by_date(top, self.reviewer_last_review_dates)

        for k in top_dict:
            self.suggested_reviewers_count[k].append(len(top_dict[k]))
            for hist in review["approve_history"]:
                reviewer = hist['userId']
                if reviewer in top_dict[k]:
                    self.prediction[k] += 1
                    break

        for hist in review["approve_history"]:
            reviewer = hist['userId']
            self.reviewer_last_review_dates[reviewer] = review['changeId']
            in_mrr = False
            for k in top_dict:
                if reviewer in top_dict[k]: 
                    if not(in_mrr):
                        self.mrr_sum += 1.0 / k
                        self.mrr_count += 1
                        in_mrr = True
        self.reviewer_count += 1

    def results(self):
        precision = collections.Counter()
        recall = collections.Counter()
        for key, value in list(self.prediction.items()):
            precision[key] = float(value) / sum(i for i in self.suggested_reviewers_count[key])
            recall[key] = float(value) / self.reviewer_count
        mrr = self.mrr_sum / self.mrr_count
        print_topN(recall)
        print_mrr(mrr)
        print('----')
        for key, value in list(self.prediction.items()):
            print(key, sum(i for i in self.suggested_reviewers_count[key]))
        print('----')
        print('----')
        for key, value in list(self.prediction.items()):
            print(key, value)
        print('----')
        print_precision(precision)
        print_recall(recall)
        return self.suggested_reviewers_count, precision, self.mrr_sum / self.mrr_count

def get_top_by_date(top, users_last_date):
    sorted_top = sorted(list(top.keys()), reverse=True)
 
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

def sorted_list_by_date(users, users_last_date):
    selected_users_dates = {u:users_last_date[u] for u in users}

    out = sorted(list(selected_users_dates.items()), key=itemgetter(1), reverse=True)
    return [o[0] for o in out]


def print_topN(prediction):
    for p in sorted(prediction): 
        print("Top %d = %f" % (p, float(prediction[p])))

def print_mrr(mrr):
    print("Mean reciprocal rank = %f" % (mrr))

def print_precision(precision_top):
    print("Precision")
    for n in sorted(precision_top): 
        print("%f" % (float(precision_top[n])))

def print_recall(recall_top):
    print("Recall")
    for n in sorted(recall_top): 
        print("%f" % (float(recall_top[n])))



def main():
    for f in sys.argv[1:]:
         reviews = parse_file(f)
         (suggested_reviewers_count, prediction, mrr), (total, transformation, update, similarity), (all_profiles_size, average_profile_size, memory_info) = process_reviews(reviews)
         print_time(total, transformation, update, similarity)
         print_size(all_profiles_size, average_profile_size)
         print(memory_info)

def print_time(total, transformation, update, similarity):
    print("Total time = %f" % (total))
    print("Total review to multiset transformation time = %f" % (transformation))
    print("Total profile update time = %f" % (update))
    print("Total profile to review similarity time = %f" % (similarity))

def print_size(all_profiles, average_profile):
    print("All profiles size in bytes = %f" % (all_profiles))
    print("Average profile size in bytes = %f" % (average_profile))

if __name__ == '__main__':
    main()
