# Analysis of vivino users preferences

## Introduction

The goal of this project is to extract useful insights about wine preferences based on data published on vivino.com about wines, users and their wine reviews / scores. 
The most interesting questions are:
1. Which factors mainly contribute to the price of wine
2. Whether user scores and/or length of reviews are caused by the price, or the other way around
3. Whether users generaly prefer domestic wines as opposed to imported wines with similar qualities (on the example of wines produced in France and Italy being the two largest wine markets)
4. Whether certain patterns can be found from the review texts written in English (e.g. depending on the user expertise, country of residence, given score, price, etc)

## Motivation

Being a wine lover but not so much of an expert, I had an impression that the more expensive a bottle is, the better is the taste, and the more likely I will like it. Still, all tastes are different, and I was always wondering if the connection between price and 'likability' is more sophisticated than that, and what other factors (if any) contribute to the price. I was also curious whether wine experts/someliers have any specific patterns in recommending wines and what words they tend to use more often to describe specific wiene types. 

To my surprise, there are not so much publicly available data about wines that can be easily extracted. [Vivino](https://www.vivino.com/) is a popular platform (that I personally use very often) containing information about wines, users, reviews, etc. It doesn't have any public API, but some data can be extracted using such instruments as Beautiful Soup, Selenium web driver, and requests library (sending GET requests to their internal API) if rate limits are respected. This encouraged me to write a program that extracts data, stores it in SQL for the ease of further data analysis. 

Permission to use and analyze data from vivino.com was granted by the company representatives.

## Technical description 

* Code written in Python;
* Interaction with the user in the command line (`argparse` library);
* Data extraction from non-public API of vivino (`requests` and `ratelimiter` libraries);
* Data stored on the local computer as binary files (`pickle` library);
* Data inserted to MySQL (MariaDB) database using AWS cloud resources (`mariadb` library);
* Data analysis and visualization in Jupyter notebooks (mainly, `numpy`, `pandas`, `matplotlib`, `seaborn`, `scikit-learn`, `keras` libraries) - work in progress

## Code design

The server side is divided into several files:

* `crawlers.py` - file containing definition of the crawler class and its methods
* `crawl.py` - file that can be run by the user from the command line to get information about wines and/or reviews (see usage below)
* `inserters.py` - file containing definition of the inserter class and its methods
* `insert.py` - file that can be run by the user from the command line to insert information about wines and/or reviews to MySQL database (see usage below)

In addition, repository includes the following files: 
* `requirements.txt` - dependencies and their versions that need to be installed prior to running the program 
* `schema.sql` - SQL statements to create an empty database
* `settings.py` - helper file to connect to AWS instance of a database
* `vivino-schema.png` - SQL schema diagram visualization
* `data_storage.ipynb` and `vivino_crawler.ipynb` - experimentation Jupyter notebooks for data extraction and storage
* `data_analysis.ipynb` - Jupyter notebook with data exploration - work in progress

## Usage

### Download the data

1. Clone this repo to your computer
2. Get into the folder using `cd wine`
3. Run `mkdir backup_data`, this folder will store general information about wines
4. Run `mkdir backup_data/reviews`, this folder will store information about reviews for chosen wines
5. Make sure you use Python 3. You may want to use a virtual environment for this
6. Install the requirements using  `pip install -r requirements.txt`
7. Choose the country and vintage years for which you would like to extract reviews
8. Run `python crawl.py [-h] [-v] [-p PATH] country years`, where:
    * `country` refers to the country name, eg. France
    * `years` refers to the required year or year range, eg. `2005:2010`(note that in case of range both start and end year are inclusive)
    * `-v`, `--verbose` is an optional argument to increase output verbosity
    * `-p`, `--path` is an optional argument where the output will be stored (by default, it chooses the folder `backup_data/`)
9. Under the hood, if not done yet, the code will first download general data about all wines (not only those chosen in item 7). Such data will be stored as a Python object (JSON) in a single pickle file at the following directory `backup_data/full_match_list`. After that, the program will filter data contained in `full_match_list` to get the desired country and year, and download all reviews for each entry. Again, data will be stored as a Python object (JSON) in a pickle file at the following directory `backup_data/full_match_list/[country]_[year]` (one file per each combination of country and year). Note that depending on the chosen country and year, pickle files can become quite heavy (e.g. for French wines of 2018 the size of the file may exceed 800 Mb).  
10. For consistency of results, the program will assume that data should be provided only for those wines that can be shipped to GBP for which vivino.com has price information, and the prices will be given in GBP. This assumption is done due to the fact that development of program took place in the United Kingdom. 

### Store data in MySQL

To facilitate analysis, data was stored in MySQL database instance using AWS cloud resources. 

In order to dump freshly loaded data (from steps 1-10) into SQL, follow the following additional steps: 

11. Obtain AWS cloud credentials from the project developer (namely, password and url) and store them in the variables `db_pass` and `db_url` in a document `secrets.py` inside the working repository. Those have not been included to the public repo for security reasons. 
12. Run `python insert.py [-w | -r] [-v] [-c] [-p PATH]`, where:
    * `-w`, `--wines` if wine information should be inserted to the database
    * `-r`, `--reviews` if review information should be inserted to the database (`wines` and `reviews` are mutually exclusive arguments)
    * `-v`, `--verbose` is an optional argument to increase output verbosity
    * `-c`, `--clean` is an optional argument to clean tables before inserting new batch of data
    * `-p`, `--path` is an optional argument where the output will be stored (by default, it chooses the folder `backup_data/`)
13. Depending on the option chosen by the user, the following tables are updated in SQL:
    * wines: `type`, `winery`, `country`, `region`, `style`, `food`, `facts`, `style_food`, `grape`, `style_grape`, `country_grape`, `wine`, `price`, `vintage`, `toplist`, `vintage_toplist`
    * reviews: `user`, `activity`, `review`, `vintage_review`
    
## Requirements

* `pandas==1.1.4`
* `numpy==1.19.4`
* `bs4==0.0.1`
* `selenium==3.141.0`
* `ratelimiter==1.2.0.post0`
* `mariadb==1.0.4`

## API reference

In order to load the data, program sends GET requests to the following vivino endpoints (neither are public, discovered using Developer Tools in browser): 
* `https://www.vivino.com/api/explore/explore?`
* `https://www.vivino.com/api/wines/[wine_id]/latest_reviews?`

## SQL schema

The complete schema of SQL database is as follows: 

![SQL schema](/vivino-schema.png)


## Format of data backup

### Wine general data

JSON stored in `backup_data/full_match_list` will have the following structure with general information about wines and their prices (overall, each vintage will store data about more than 100 features): 
```
[
{'vintage': {'id': 111604237,
  'seo_name': 'esporao-alandra-tinto-2016',
  'name': 'Esporão Alandra Tinto 2016',
  'statistics': {'status': 'Normal',
   'ratings_count': 2142,
   'ratings_average': 3.2,
   'labels_count': 17293},
  'image': ...,
  'wine': {'id': 1105374,
   'name': 'Alandra Tinto',
   'seo_name': 'alandra-tinto',
   'type_id': 1,
   'vintage_type': 0,
   'is_natural': False,
   'region': {'id': 1395,
    'name': 'Alentejo',
    'name_en': '',
    'seo_name': 'alentejo',
    'country': {'code': 'pt',
     'name': 'Portugal',
     ...,
     'most_used_grapes': [{'id': 67,
       'name': 'Touriga Nacional',
       'seo_name': 'touriga-nacional',
       'has_detailed_info': True,
       'wines_count': 70337},
      ...]},
    ...,
   'winery': {'id': 3155,
    'name': 'Esporão',
    'seo_name': 'esporao',
    'status': 0},
   'taste': {'structure': {'acidity': 3.1399224,
     'fizziness': None,
     'intensity': 3.7395344,
     'sweetness': 1.8591489,
     'tannin': 2.9908836,
     'user_structure_count': 540,
     'calculated_structure_count': 184},
    'flavor': [...]},
   'statistics': ...,
   'style': {'id': 2,
    'seo_name': 'portuguese-alentejo-red',
    ...
    'body': 4,
    'body_description': 'Full-bodied',
    'acidity': 3,
    'acidity_description': 'High',
    'country': ...,
    'wine_type_id': 1,
    'food': [...],
    'grapes': [...],
    'region': ...,
   'has_valid_ratings': True},
  'year': 2016,
  'grapes': None,
  'has_valid_ratings': True},
 'price': {'id': 21003666,
  'amount': 1.47,
  'discounted_from': None,
  ...,
  'currency': {'code': 'GBP',
   'name': 'British Pounds',
   ...},
  'bottle_type': {'id': 3,
   'name': '1/2 Bottle (0.375 l)',
   ...}},
 'prices': [...]}

]
```
### Wine review data 

JSON stored in `backup_data/full_match_list/[country]_[year]` will contain review records per each vintage from a given country of a given year. JSON with reviews will have the following structure with general information:
```
{
"reviews": [{"activity": {"id": 331298026, 
                          "statistics": {"likes_count": 58, 
                                         "comments_count": 3}
                          },
             "aggregated": true,
             "created_at": "2019-05-18T21:36:04.000Z",
             "id": 127250463,
             "language": "en",
             "note": "Excellent red! Complex blend of Primitivo, Sangiovese, Negroamaro and plus 2 more. Deep. Complex. Excellent balance!4.5* Russian- in comments ",
             "rating": 4.5,
             "tagged_note": "Excellent red! Complex blend of Primitivo, Sangiovese, Negroamaro and plus 2 more. Deep. Complex. Excellent balance!4.5* Russian- in comments ",
             "user": {"alias": "Alexandre Kondrashov YPO",
                      "background_image": null,
                      "id": 1183892,
                      "is_featured": false,
                      "seo_name": "alexandre_ko",
                      "statistics": {"followers_count": 2040, 
                                     "followings_count": 2274, 
                                     "ratings_count": 3085, 
                                     "ratings_sum": 11981,
                                     "reviews_count": 2727
                                     },
                      "visibility": "all"
                      },
             "vintage": {"id": 150615005, 
                         "seo_name": "farnese-edizione-cinque-autoctoni-2017",…}
             },
             {...},
             ...]
}
```