import json
import pandas as pd
from pyspark.sql import SparkSession
from collections import Counter

# Mapper for Beer Dimension
class BeerMapper:
    def map(self, row):
        try:
            key = row["Name"]
            beer_fact = "ABV"  # Example field name, change it as necessary
            fact = float(row[beer_fact]) if row[beer_fact] else 0.0
            yield key, ("Beer", fact)
        except ValueError:
            pass

# Reducer for Beer Dimension
class BeerReducer:
    def reduce(self, values):
        abv_counts = {}
        for value in values:
            abv = value[1]
            if isinstance(abv, float):
                if abv in abv_counts:
                    abv_counts[abv] += 1
                else:
                    abv_counts[abv] = 1
        return ("Beer", abv_counts)

# Driver for Beer Dimension
class BeerDimensionDriver:
    def __init__(self, data_file, output_file):
        self.data_file = data_file
        self.output_file = output_file
        self.spark = SparkSession.builder \
            .appName("Beer Analysis - Beer Dimension") \
            .getOrCreate()

    def run(self):
        df = self.spark.read.csv(self.data_file, header=True, inferSchema=True)
        beer_dimension = df.rdd.flatMap(BeerMapper().map)
        reduced_beer_data = beer_dimension.groupByKey().mapValues(BeerReducer().reduce).collect()

        unique_abvs = {}
        for beer in reduced_beer_data:
            abv_dict = beer[1][1]
            for abv, count in abv_dict.items():
                if abv in unique_abvs:
                    unique_abvs[abv] += count
                else:
                    unique_abvs[abv] = count
        
        beer_df = pd.DataFrame(list(unique_abvs.items()), columns=["ABV", "Count"])
        print("Beer Dimension:")
        print(beer_df)
        
        with open(self.output_file, 'w') as f:
            json.dump(unique_abvs, f, indent=4)

# Mapper for Review Dimension
class ReviewMapper:
    def map(self, row):
        try:
            key = row["Name"]
            review_overall = float(row["review_overall"]) if row["review_overall"] else 0.0
            yield key, ("Review", review_overall)
        except ValueError:
            pass

# Reducer for Review Dimension
class ReviewReducer:
    def reduce(self, values):
        total_review = 0.0
        count = 0
        for value in values:
            total_review += value[1]
            count += 1
        average_review = total_review / count if count > 0 else 0.0
        return ("Review", average_review)

# Driver for Review Dimension
class ReviewDimensionDriver:
    def __init__(self, data_file, output_file):
        self.data_file = data_file
        self.output_file = output_file
        self.spark = SparkSession.builder \
            .appName("Beer Analysis - Review Dimension") \
            .getOrCreate()

    def run(self):
        df = self.spark.read.csv(self.data_file, header=True, inferSchema=True)
        review_dimension = df.rdd.flatMap(ReviewMapper().map)
        reduced_review_data = review_dimension.groupByKey().mapValues(ReviewReducer().reduce).collect()
        
        review_df = pd.DataFrame(reduced_review_data, columns=["Beer", "Average Overall Review"])
        print("Review Dimension:")
        print(review_df)

        beer_count_df = df.groupBy("review_overall").count().toPandas()
        beer_count_df = beer_count_df.rename(columns={"review_overall": "Review Overall", "count": "Beer Count"})
        beer_count_df = beer_count_df.sort_values(by="Beer Count", ascending=False)
        print("\nBeer Count by Review Overall:")
        print(beer_count_df)

        df.createOrReplaceTempView("beers")
        top_5_highest_reviews = self.spark.sql("SELECT Name, review_overall FROM beers ORDER BY review_overall DESC LIMIT 5")
        top_5_lowest_reviews = self.spark.sql("SELECT Name, review_overall FROM beers ORDER BY review_overall ASC LIMIT 5")

        top_5_highest_reviews_pd = top_5_highest_reviews.toPandas()
        top_5_lowest_reviews_pd = top_5_lowest_reviews.toPandas()

        print("\nTop 5 Beers with Highest Reviews:")
        print(top_5_highest_reviews_pd)
        print("\nTop 5 Beers with Lowest Reviews:")
        print(top_5_lowest_reviews_pd)

        with open(self.output_file, 'w') as f:
            json.dump(reduced_review_data, f, indent=4)

# Mapper for Taste Dimension
class TasteMapper:
    def map(self, row):
        try:
            key = row["Name"]
            bitter = float(row["Bitter"]) if row["Bitter"].replace('.', '', 1).isdigit() else None
            alcohol = float(row["Alcohol"]) if row["Alcohol"].replace('.', '', 1).isdigit() else None
            if bitter is not None and alcohol is not None:
                yield key, (alcohol, bitter)
        except ValueError:
            pass

# Reducer for Taste Dimension
class TasteReducer:
    def __init__(self):
        self.alcohol_total = 0
        self.bitterness_total = 0
        self.count = 0

    def reduce(self, values):
        for value in values:
            alcohol, bitterness = value
            self.alcohol_total += alcohol
            self.bitterness_total += bitterness
            self.count += 1

    def get_average(self):
        if self.count == 0:
            return 0, 0
        average_alcohol = self.alcohol_total / self.count
        average_bitterness = self.bitterness_total / self.count
        return average_alcohol, average_bitterness

# Driver for Taste Dimension
class TasteDimensionDriver:
    def __init__(self, data_file, output_file_path):
        self.data_file = data_file
        self.output_file_path = output_file_path
        self.spark = SparkSession.builder \
            .appName("Beer Analysis - Taste Dimension") \
            .getOrCreate()

    def run(self):
        df = self.spark.read.csv(self.data_file, header=True, inferSchema=True)
        taste_dimension = df.rdd.flatMap(TasteMapper().map)
        
        reducer = TasteReducer()
        input_data = taste_dimension.collect()
        for name, value in input_data:
            reducer.reduce([value])
        
        average_alcohol, average_bitterness = reducer.get_average()
        print("Average Alcohol:", average_alcohol)
        print("Average Bitterness:", average_bitterness)

        result = {'Average Alcohol': average_alcohol, 'Average Bitterness': average_bitterness}
        with open(self.output_file_path, 'w') as f:
            json.dump(result, f)
