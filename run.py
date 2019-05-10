import os

from flask_bootstrap import Bootstrap
from pyspark import SparkContext, SparkConf

from app import create_app


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

    return sc


bootstrap = Bootstrap()

if __name__ == "__main__":
    # init spark context and load libraries
    sc = init_spark_context()
    dataset_path = os.path.join('datasets', 'ml-latest-small')
    app = create_app(sc, dataset_path)
    bootstrap.init_app(app)
    # start web server
    app.run()
