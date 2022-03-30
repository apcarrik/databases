"""
If you modify anything besides the areas marked as *** CODE HERE *** then it is your responsibility to change it back. 
You may be unable to submit or face errors due to formatting errors or otherwise resulting from such changes.

This has been tested with python 3.8-3.10 on Unix and MacOS terminal (i.e. 'python3 answers.py').
"""

from pymongo import MongoClient 
from bson import json_util 

class Answers:

    def __init__(self):
        self.dblp = MongoClient("localhost", 27017)["dblp"]    
        self.article = self.dblp['Article']
        self.inproceedings = self.dblp['Inproceedings']

    def run(self):

        methods = [func for func in dir(self) if callable(getattr(self, func)) and func.startswith('q')]
        method_names = [str(func) for func in methods]
        choices = ['all'] + method_names 
        choice = None 
        while not choice in choices: 
             choice = input(f"Enter one of {choices}\n")

        qus = method_names if choice == "all" else [choice]

        for qu in qus: 
            func = getattr(self, qu)
            res = func() 
            if not isinstance(res, list):
                print(f"Please re-read all the formatting instructions for {qu}. For every question you must return a list.")
                break 
            file_name = f"{qu}.json"
            with open(file_name, 'w') as f: 
                # json_util.dumps is specially used instead of json.dumps because 'ObjectID' is not JSON serializable by default!
                    # however, for all of these questions (except sample q_test) you do NOT ever need to nor should return any ObjectID
                f.write(json_util.dumps(res, indent=4))
            print(f"Successfully ran {qu} and made {file_name}")
        
    def q_test(self):
        """
        Test query 
        """

        res = self.article.find({}).limit(10)

        # Res is a cursor! Make sure to read MongoDB documentation to be extra careful about what returns results directly (rare if any) and what returns a cursosr
        print(res) 

        return [_ for _ in res]

    def q2(self):
        """
        Counts of 'Article' and 'Inproceedings'
        End result must be an array containing an object of the form {'article_count': ..., 'inproceedings_count': ...}
        """

        # *** CODE HERE *** 
        count_article = self.article.count_documents({})
        count_inproceedings = self.inproceedings.count_documents({})

        return [{
            'article_count': count_article, 
            'inproceedings_count': count_inproceedings,
        }]

    def q3(self):
        """
        Add a column 'area' to Inproceedings and return count of all area values (including 'UNKNOWN')
        End result must be an array containing an object of the form {'area': ..., 'count': ...}
        """

        # Provided for your convenience...
        area_to_title = {
            'Database': ['VLDB', 'ICDE', 'PODS', 'SIGMOD Conference'],
            'Theory': ['STOC', 'FOCS', 'SODA', 'ICALP'],
            'Systems': ['ISCA', 'HPCA', 'PLDI', 'SIGCOMM'],
            'ML-AI': ['ICML', 'NIPS', 'AAAI', 'IJCAI']
        }


        # *** CODE HERE *** 
        self.inproceedings.update_many({}, {'$set' : {'area' : 'UNKNOWN'}})

        for area in area_to_title:
            for conference in area_to_title[area]:
                self.inproceedings.update_many({'booktitle' : conference}, {'$set' : {'area' : area}})

        ans = []
        for area in ['Database', 'Theory', 'Systems', 'ML-AI', 'UNKNOWN']:
            ans.append({'area': area, 'count': self.inproceedings.count_documents({'area' : area})})

        ans.sort(key = lambda d: d['area'])

        return ans 

    def q4a(self):

        """
        Top 20 authors published most # Database papers (Inproceedings).
        Must be done in ONE aggregation.
        Result must be an array containing objects of the form {'author': ..., 'num_papers': ...} sorted in descending order by num_papers, any ties broken in ascending order by author.
        """

        # *** CODE HERE *** 
        res = self.inproceedings.aggregate([
            {'$match' : {'area' : 'Database'}},
            {'$unwind' : "$authors"},
            {'$group' : {'_id' : '$authors', 'num_papers' : {'$sum' : 1}}},
            {'$sort' : {'num_papers' : -1, '_id' : 1}},
            {'$limit' : 20},
            {'$project' : {'author' : '$_id', '_id' : 0, 'num_papers' : 1}}
        ])


        return [_ for _ in res]

    def q4b(self):
        """
        Find # authors who published in exactly two areas (not counting UNKNOWN).
        Must be done in ONE aggregation.
        Result must be an array composed of objects of the structure {'author': ...} sorted in ascending order by author name.
        """

        res = self.inproceedings.aggregate([
            {'$match' : {'area' : {'$ne' : 'UNKNOWN'}}},
            {'$unwind' : "$authors"},
            {'$group' : {'_id' : {'authors' : '$authors', 'area' : '$area'}}},
            {'$group' : {'_id' : '$_id.authors', 'count_area' : {'$sum' : 1}}},
            {'$match' : {'count_area' : 2}},
            {'$project' : {'author' : '$_id', '_id' : 0}},
            {'$sort' : {'author' : 1}},
        ])
        return [_ for _ in res]
    
    def q4c(self):
        """
        Find the top 5 authors who published the maximum number of journal papers since 2000 among the top 20 authors who published at least one conference 'Database' paper.
        Must be done in ONE aggregation.
        Result must be an array composed of objects of the structure {'author': ..., 'num_papers': ...} sorted in descending order of num_papers, ties broken in ascending order by author name.
        """
        
        # *** CODE HERE *** 
        res = self.inproceedings.aggregate([
            {'$match' : {'area' : 'Database'}},
            {'$unwind' : "$authors"},
            {'$group' : {'_id' : '$authors', 'num_papers' : {'$sum' : 1}}},
            {'$sort' : {'num_papers' : -1, '_id' : 1}},
            {'$limit' : 20},
            {'$project' : {'author' : '$_id', '_id' : 0, 'num_papers' : 1}},
            {'$lookup' : {'from' : 'Article',
                          'localField' : 'author',
                          'foreignField' : 'authors',
                          'pipeline' : [{'$match' : {'year' : {'$gte' : 2000}}}, {'$unwind' : '$authors'}],
                          "as" : "articles"}},
            {'$unwind' : "$articles"},
            {'$match' : {'$expr' : {'$eq' : ["$author", "$articles.authors"]}}},
            {'$group' : {'_id' : '$author', 'num_papers' : {'$sum' : 1}}},
            {'$sort' : {'num_papers' : -1, '_id' : 1}},
            {'$limit' : 5},
            {'$project' : {'author' : '$_id', '_id' : 0, 'num_papers' : 1}}
        ])
        return [_ for _ in res]

if __name__ == "__main__":

    answers = Answers() 
    answers.run()